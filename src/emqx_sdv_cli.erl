%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_cli).

-export([cmd/1]).

-include("emqx_sdv.hrl").

cmd(["show-config" | More]) ->
    emqx_ctl:print("~ts~n", [get_config(More)]);
cmd(_) ->
    emqx_ctl:usage([
        {"show-config [origin|inuse] [--json]",
            "Show current config, 'origin' for original config, "
            "'inuse' for in-use (parsed) config, add '--json' for JSON format."}
    ]).

get_config([]) ->
    get_config(["origin"]);
get_config(["origin" | MaybeJSON]) ->
    Config = emqx_plugin_helper:get_config(?PLUGIN_NAME_VSN),
    case MaybeJSON of
        "--json" ->
            emqx_utils_json:encode(Config);
        _ ->
            hocon_pp:do(Config)
    end;
get_config(["inuse" | MaybeJSON]) ->
    Config = emqx_sdv_config:get(),
    case MaybeJSON of
        "--json" ->
            emqx_utils_json:encode(Config);
        _ ->
            io_lib:format("~p~n", [hocon_pp:do(Config)])
    end;
get_config(_) ->
    error("invalid_config_key").
