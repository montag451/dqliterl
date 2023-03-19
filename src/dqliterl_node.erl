-module(dqliterl_node).

-behaviour(gen_server).

-export([
    new/3, new/4,
    start/1,
    destroy/1,
    recover/2,
    id/1,
    is_supervised/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_continue/2,
    handle_info/2,
    terminate/2
]).

-include("dqliterl.hrl").

-type node_id() :: dqliterl_proto:node_id().
-type node_info() :: dqliterl_proto:node_info().
-type node_ref() :: pid() | node_id().
-type node_addr() :: unicode:chardata().
-type node_dir() :: unicode:chardata().
-type node_opt() ::
    {bind_addr, unicode:chardata()}
    | {network_latency, pos_integer()}
    | {failure_domain, non_neg_integer()}
    | {snapshot_params, {non_neg_integer(), non_neg_integer()}}
    | {disk_mode, boolean()}.
-type node_opts() :: [node_opt()].

-type new_opts() :: [supervised | {supervised, boolean()} | node_opt()].
-type new_ret() :: {ok, node_ref()} | ignore | {error, term()}.

-type command_failed() :: {command_failed, atom(), term()}.
-type unexpected_port_msg() :: {unexpected_port_msg, term()}.
-type send_command_ret() ::
    ok
    | {ok, term()}
    | {error, command_failed() | unexpected_port_msg()}.

-record(state, {
    id :: node_id(),
    supervised = false :: boolean(),
    started = false :: boolean(),
    port :: port() | undefined
}).
-type state() :: #state{}.

-define(REGISTRY, dqliterl_node_registry).
-define(SUPERVISOR, dqliterl_node_instance_sup).

-define(is_exit_port_msg(Port, Msg),
    (element(1, Msg) =:= 'EXIT' andalso element(2, Msg) =:= Port)
).
-define(is_port_msg(Port, Msg),
    (element(1, Msg) =:= Port orelse ?is_exit_port_msg(Port, Msg))
).

%% API functions

-spec new(Id, Addr, Dir) -> new_ret() when
    Id :: node_id(),
    Addr :: node_addr(),
    Dir :: node_dir().
new(Id, Addr, Dir) ->
    new(Id, Addr, Dir, [supervised]).

-spec new(Id, Addr, Dir, Opts) -> new_ret() when
    Id :: node_id(),
    Addr :: node_addr(),
    Dir :: node_dir(),
    Opts :: new_opts().
new(Id, Addr, Dir, Opts) ->
    Supervised =
        case proplists:is_defined(supervised, Opts) of
            false ->
                true;
            true ->
                proplists:get_bool(supervised, Opts)
        end,
    NodeOpts = proplists:delete(supervised, Opts),
    case Supervised of
        false ->
            Via = {via, ?REGISTRY, Id},
            InitOpts = {false, Id, Addr, Dir, NodeOpts},
            gen_server:start_link(Via, ?MODULE, InitOpts, []);
        true ->
            case ?SUPERVISOR:start_child(child_spec(Id, Addr, Dir, NodeOpts)) of
                {ok, _} ->
                    {ok, Id};
                {ok, _, _} ->
                    {ok, Id};
                Error ->
                    Error
            end
    end.

-spec start(NodeRef :: node_ref()) -> ok | {error, term()}.
start(NodeRef) ->
    call(NodeRef, start).

-spec destroy(NodeRef :: node_ref()) -> ok.
destroy(NodeRef) ->
    case is_supervised(NodeRef) of
        false ->
            call(NodeRef, destroy);
        true ->
            Id = id(NodeRef),
            Ret = call(NodeRef, destroy),
            ok = ?SUPERVISOR:delete_child(Id),
            Ret
    end.

-spec recover(NodeRef, Cluster) -> ok | {error, term()} when
    NodeRef :: node_ref(),
    Cluster :: [node_info()].
recover(NodeRef, Cluster) ->
    call(NodeRef, {recover, Cluster}).

-spec id(NodeRef :: node_ref()) -> node_id().
id(NodeRef) ->
    call(NodeRef, id).

-spec is_supervised(NodeRef :: node_ref()) -> boolean().
is_supervised(NodeRef) ->
    call(NodeRef, is_supervised).

%% gen_server callbacks

-spec init({boolean(), node_id(), node_addr(), node_dir(), node_opts()}) ->
    {ok, state()} | {ok, state(), {continue, start}} | {stop, term()}.
init({Supervised, Id, Addr, Dir, Opts}) ->
    process_flag(trap_exit, true),
    case spawn_node(Id, Addr, Dir, Opts) of
        {ok, Port} ->
            State = #state{id = Id, supervised = Supervised, port = Port},
            case Supervised of
                true ->
                    {ok, State, {continue, start}};
                false ->
                    {ok, State}
            end;
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(start, _From, #state{started = true} = State) ->
    {reply, {error, already_started}, State};
handle_call(start, _From, State) ->
    StateTrans = fun
        (OldState, ok) -> OldState#state{started = true};
        (OldState, {error, _}) -> OldState
    end,
    do_command_and_reply(start, no_arg, State, StateTrans);
handle_call(destroy, _From, #state{supervised = Supervised} = State) ->
    case do_destroy(State) of
        {ok, State2} when Supervised ->
            {stop, {shutdown, destroy}, ok, State2};
        {ok, State2} ->
            {stop, normal, ok, State2};
        {{error, _} = Error, State2} when Supervised ->
            {stop, {shutdown, destroy}, Error, State2};
        {{error, Reason} = Error, State2} ->
            {stop, Reason, Error, State2}
    end;
handle_call({recover, _Cluster}, _From, #state{started = true} = State) ->
    {reply, {error, recover_on_started_node}, State};
handle_call({recover, Cluster}, _From, State) ->
    do_command_and_reply(recover, Cluster, State, keep);
handle_call(id, _From, #state{id = Id} = State) ->
    {reply, Id, State};
handle_call(is_supervised, _From, #state{supervised = Supervised} = State) ->
    {reply, Supervised, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_continue(start, State) ->
    case do_command(start, State) of
        {ok, State2} ->
            {noreply, State2#state{started = true}};
        {Error, State2} ->
            {stop, Error, State2}
    end.

handle_info(Msg, #state{port = Port} = State) when ?is_port_msg(Port, Msg) ->
    Reason = unexpected_port_msg(Msg),
    {stop, Reason, update_state_from_error(State, {error, Reason})}.

-spec terminate(term(), state()) -> _.
terminate(_Reason, #state{port = undefined}) ->
    ok;
terminate({unexpected_port_msg, {Port, {data, _}}}, #state{port = Port}) ->
    % The port is misbehaving, close it without destroying it.
    catch erlang:port_close(Port);
terminate(_Reason, State) ->
    {ok, _} = do_destroy(State).

%% internal functions

-spec child_spec(Id, Addr, Dir, Opts) -> supervisor:child_spec() when
    Id :: node_id(),
    Addr :: node_addr(),
    Dir :: node_dir(),
    Opts :: node_opts().
child_spec(Id, Addr, Dir, Opts) ->
    #{
        id => Id,
        modules => [?MODULE],
        restart => transient,
        start =>
            {
                gen_server, start_link, [
                    {via, ?REGISTRY, Id},
                    ?MODULE,
                    {true, Id, Addr, Dir, Opts},
                    []
                ]
            }
    }.

-spec call(node_ref(), term()) -> term().
call(NodeRef, Req) when is_pid(NodeRef) ->
    gen_server:call(NodeRef, Req);
call(NodeRef, Req) when is_integer(NodeRef) ->
    gen_server:call({via, ?REGISTRY, NodeRef}, Req).

-spec spawn_node(node_id(), node_addr(), node_dir(), node_opts()) ->
    {ok, port()} | {error, term()}.
spawn_node(Id, Addr, Dir, Opts) ->
    Opts2 = [{id, Id}, {addr, Addr}, {dir, Dir} | Opts],
    NodeBin = filename:join([code:priv_dir(dqliterl), "bin", "dqliterl_node_debug.sh"]),
    Port = open_port(
        {spawn_executable, NodeBin}, [{packet, 4}, binary, exit_status]
    ),
    case send_command(Port, create, Opts2) of
        ok ->
            {ok, Port};
        Error ->
            Error
    end.

-spec do_destroy(state()) -> {send_command_ret(), state()}.
do_destroy(#state{port = Port} = State) ->
    case do_stop(State) of
        {ok, State2} ->
            case do_command(destroy, State2) of
                {ok, State3} ->
                    case wait_for_port_msg(Port, {exit_status, 0}) of
                        ok ->
                            {ok, State3#state{port = undefined}};
                        {unexpected_port_msg, _} = Reason ->
                            Error = {error, Reason},
                            {Error, update_state_from_error(State3, Error)}
                    end;
                {{error, _}, _} = Res ->
                    Res
            end;
        {{error, _}, _} = Res ->
            Res
    end.

-spec do_stop(state()) -> {send_command_ret(), state()}.
do_stop(#state{started = false} = State) ->
    {ok, State};
do_stop(State) ->
    case do_command(stop, State) of
        {ok, State2} ->
            {ok, State2#state{started = false}};
        {{error, _}, _} = Res ->
            Res
    end.

-spec do_command_and_reply(CmdName, Args, State, StateTrans) -> Ret when
    CmdName :: atom(),
    Args :: no_arg | [term()],
    State :: state(),
    StateTrans :: keep | fun((state(), send_command_ret()) -> state()),
    Ret :: {reply, Reply, state()} | {stop, Reason, {error, Reason}, state()},
    Reply :: ok | {ok, term()} | command_failed(),
    Reason :: unexpected_port_msg().
do_command_and_reply(CmdName, Args, State, StateTrans) ->
    {Res, State2} = do_command(CmdName, Args, State),
    State3 =
        case StateTrans of
            keep ->
                State2;
            _ ->
                StateTrans(State2, Res)
        end,
    case Res of
        ok ->
            {reply, Res, State3};
        {ok, _} ->
            {reply, Res, State3};
        {error, {command_failed, _, _}} ->
            {reply, Res, State3};
        {error, Reason} ->
            {stop, Reason, Res, State3}
    end.

-spec do_command(atom(), state()) -> {send_command_ret(), state()}.
do_command(CmdName, State) ->
    do_command(CmdName, no_arg, State).

-spec do_command(atom(), no_arg | [term()], state()) ->
    {send_command_ret(), state()}.
do_command(CmdName, Args, #state{port = Port} = State) when is_port(Port) ->
    case send_command(Port, CmdName, Args) of
        ok ->
            {ok, State};
        {ok, _} = Res ->
            {Res, State};
        Error ->
            {Error, update_state_from_error(State, Error)}
    end.

-spec update_state_from_error(state(), term()) -> state().
update_state_from_error(#state{port = Port} = State, Error) ->
    case Error of
        {error, {unexpected_port_msg, {Port, {exit_status, _}}}} = Error ->
            State#state{port = undefined};
        {error, {unexpected_port_msg, {Port, closed}}} = Error ->
            State#state{port = undefined};
        {error, {unexpected_port_msg, {Port, connected}}} = Error ->
            State#state{port = undefined};
        {error, {unexpected_port_msg, {'EXIT', Port, _}}} = Error ->
            State#state{port = undefined};
        _ ->
            State
    end.

-spec send_command(port(), atom(), no_arg | [term()]) -> send_command_ret().
send_command(Port, CmdName, Args) when is_port(Port) ->
    Cmd =
        case Args of
            no_arg ->
                CmdName;
            _ ->
                {CmdName, Args}
        end,
    Port ! {self(), {command, term_to_binary(Cmd)}},
    case wait_for_port_msg(Port, data) of
        {ok, Bin} ->
            case binary_to_term(Bin) of
                ok ->
                    ok;
                {ok, _} = Res ->
                    Res;
                {error, Error} ->
                    {error, {command_failed, CmdName, Error}}
            end;
        Reason ->
            {error, Reason}
    end.

-spec wait_for_port_msg(port(), term()) ->
    ok | {ok, term()} | unexpected_port_msg().
wait_for_port_msg(Port, Expected) ->
    receive
        {Port, Expected} ->
            ok;
        {Port, {Expected, Term}} ->
            {ok, Term};
        Msg when ?is_port_msg(Port, Msg) ->
            unexpected_port_msg(Msg)
    end.

-spec unexpected_port_msg(term()) -> {unexpected_port_msg, term()}.
unexpected_port_msg(Msg) ->
    {unexpected_port_msg, Msg}.
