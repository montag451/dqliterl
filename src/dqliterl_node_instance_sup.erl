-module(dqliterl_node_instance_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    start_child/1,
    stop_child/1,
    delete_child/1
]).

-export([init/1]).

-define(SERVER, ?MODULE).

%% API functions

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec start_child(Spec) -> Ret when
    Spec :: supervisor:child_spec(),
    Ret :: supervisor:startchild_ret().
start_child(Spec) ->
    supervisor:start_child(?SERVER, Spec).

-spec stop_child(Id :: term()) -> ok.
stop_child(Id) ->
    ok = supervisor:terminate_child(?SERVER, Id),
    ok = supervisor:delete_child(?SERVER, Id).

-spec delete_child(Id :: term()) -> ok.
delete_child(Id) ->
    ok = supervisor:delete_child(?SERVER, Id).

%% supervisor callbacks

init([]) ->
    SupFlags = #{strategy => one_for_one},
    {ok, {SupFlags, []}}.
