%%%-------------------------------------------------------------------
%% @doc dqliterl public API
%% @end
%%%-------------------------------------------------------------------

-module(dqliterl_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    dqliterl_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
