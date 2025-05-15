%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_fanout_cli).

%% This is an example on how to extend `emqx ctl` with your own commands.

-export([cmd/1]).

cmd(["get-config"]) ->
    Config = emqx_sdv_fanout:get_config(),
    emqx_ctl:print("~s~n", [emqx_utils_json:encode(Config)]);
cmd(_) ->
    emqx_ctl:usage([{"get-config", "get current config"}]).
