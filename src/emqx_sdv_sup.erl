%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-include("emqx_sdv.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 10,
        period => 5
    },
    InflightTableOwner = #{
        id => emqx_sdv_fanout_inflight,
        start => {emqx_sdv_fanout_inflight, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_sdv_fanout_inflight]
    },
    ConfigChildSpec = #{
        id => emqx_sdv,
        start => {emqx_sdv, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_sdv]
    },
    GcChildSpec = #{
        id => emqx_sdv_fanout_gc,
        start => {emqx_sdv_fanout_gc, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_sdv_fanout_gc]
    },
    PoolModule = ?DISPATCHER_POOL,
    PoolType = hash,
    PoolSize = resolve_pool_size(),
    MFA = {PoolModule, start_link, []},
    Pool = PoolModule,
    SupArgs = [Pool, PoolType, PoolSize, MFA],
    PoolSupSpec = emqx_pool_sup:spec(emqx_sdv_fanout_dispatcher_sup, SupArgs),
    Children =
        case mria_config:whoami() of
            core ->
                [ConfigChildSpec, InflightTableOwner, PoolSupSpec, GcChildSpec];
            _ ->
                [ConfigChildSpec, InflightTableOwner, PoolSupSpec]
        end,
    {ok, {SupFlags, Children}}.

resolve_pool_size() ->
    %% Get config from emqx_plugin_helper, but not from
    %% emqx_sdv_config because it's not initialized yet
    Config = emqx_plugin_helper:get_config(?PLUGIN_NAME_VSN),
    ConfigedSize = maps:get(<<"dispatcher_pool_size">>, Config),
    resolve_pool_size(ConfigedSize).

resolve_pool_size(0) ->
    erlang:system_info(schedulers);
resolve_pool_size(N) when is_integer(N) ->
    N.
