-module(dqliterl_node_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

%% API functions

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% supervisor callbacks

init([]) ->
    SupFlags = #{strategy => rest_for_one},
    {ok, {SupFlags, child_specs()}}.

%% internal functions

-spec child_specs() -> [supervisor:child_spec()].
child_specs() ->
    [
        #{
            id => dqliterl_node_registry,
            start => {dqliterl_node_registry, start_link, []}
        },
        #{
            id => dqliterl_node_instance_sup,
            start => {dqliterl_node_instance_sup, start_link, []},
            type => supervisor
        }
    ].
