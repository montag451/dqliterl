-module(dqliterl_node_registry).

-behaviour(gen_server).

-export([
    start_link/0,
    register_name/2,
    unregister_name/1,
    whereis_name/1,
    send/2
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-record(state, {
    names :: #{term() => {pid(), reference()}},
    refs :: #{reference() => term()}
}).
-type state() :: #state{}.

-define(SERVER, ?MODULE).

%% API functions

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec register_name(Name :: term(), Pid :: pid()) -> yes | no.
register_name(Name, Pid) ->
    call({register_name, Name, Pid}).

-spec unregister_name(Name :: term()) -> ok.
unregister_name(Name) ->
    call({unregister_name, Name}).

-spec whereis_name(Name :: term()) -> pid() | undefined.
whereis_name(Name) ->
    call({whereis_name, Name}).

-spec send(Name :: term(), Msg :: term()) -> pid().
send(Name, Msg) ->
    case whereis_name(Name) of
        Pid when is_pid(Pid) ->
            Pid ! Msg,
            Pid;
        undefined ->
            exit({badarg, {Name, Msg}})
    end.

%% gen_server callbacks

-spec init([]) -> {ok, state()}.
init([]) ->
    {ok, #state{names = #{}, refs = #{}}}.

handle_call({register_name, Name, Pid}, _From, State) ->
    Names = State#state.names,
    case Names of
        #{Name := {Pid, _}} ->
            case is_process_alive(Pid) of
                true ->
                    {reply, no, State};
                false ->
                    State2 = do_unregister_name(Name, State),
                    {reply, yes, do_register_name(Name, Pid, State2)}
            end;
        _ ->
            {reply, yes, do_register_name(Name, Pid, State)}
    end;
handle_call({unregister_name, Name}, _From, State) ->
    {reply, ok, do_unregister_name(Name, State)};
handle_call({whereis_name, Name}, _From, #state{names = Names} = State) ->
    case Names of
        #{Name := {Pid, _}} ->
            {reply, Pid, State};
        _ ->
            {reply, undefined, State}
    end.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, _Pid, _Info}, State) ->
    Names = State#state.names,
    Refs = State#state.refs,
    case Refs of
        #{Ref := Name} ->
            {noreply, State#state{
                names = maps:remove(Name, Names),
                refs = maps:remove(Ref, Refs)
            }};
        _ ->
            {noreply, State}
    end.

%% internal functions

-spec call(term()) -> term().
call(Req) ->
    gen_server:call(?SERVER, Req).

-spec do_register_name(term(), pid(), state()) -> state().
do_register_name(Name, Pid, State) ->
    Names = State#state.names,
    Refs = State#state.refs,
    Ref = erlang:monitor(process, Pid),
    State#state{
        names = Names#{Name => {Pid, Ref}},
        refs = Refs#{Ref => Name}
    }.

-spec do_unregister_name(term(), state()) -> state().
do_unregister_name(Name, State) ->
    Names = State#state.names,
    Refs = State#state.refs,
    case Names of
        #{Name := {_, Ref}} ->
            true = erlang:demonitor(Ref),
            State#state{
                names = maps:remove(Name, Names),
                refs = maps:remove(Ref, Refs)
            };
        _ ->
            State
    end.
