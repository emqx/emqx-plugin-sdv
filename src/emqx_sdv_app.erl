%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([
    start/2,
    stop/1
]).

-export([
    on_config_changed/2,
    on_health_check/1
]).

-include("emqx_sdv.hrl").

start(_StartType, _StartArgs) ->
    create_tables(),
    {ok, Sup} = emqx_sdv_sup:start_link(),
    emqx_sdv:hook(),
    emqx_ctl:register_command(sdv, {emqx_sdv_cli, cmd}),
    {ok, Sup}.

stop(_State) ->
    emqx_ctl:unregister_command(sdv),
    emqx_sdv:unhook().

on_config_changed(OldConfig, NewConfig) ->
    emqx_sdv:on_config_changed(OldConfig, NewConfig).

on_health_check(Options) ->
    emqx_sdv:on_health_check(Options).

create_tables() ->
    ok = emqx_sdv_fanout_data:create_tables(),
    ok = emqx_sdv_fanout_ids:create_tables(),
    ok.
