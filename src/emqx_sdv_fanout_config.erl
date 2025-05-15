%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_fanout_config).

-compile({no_auto_import, [get/0, put/1]}).
-export([get/0, put/1, parse/1]).
-export([get_data_retention/0]).

-include("emqx_sdv_fanout.hrl").

-spec get() -> map().
get() ->
    persistent_term:get(?MODULE).

-spec put(map()) -> ok.
put(Parsed) ->
    persistent_term:put(?MODULE, Parsed).

-spec parse(map()) -> map().
parse(Config) ->
    maps:fold(
        fun(Key, Value, Acc) ->
            {Key1, Parsed} = parse(Key, Value),
            maps:put(Key1, Parsed, Acc)
        end,
        #{},
        Config
    ).

-spec get_data_retention() -> integer().
get_data_retention() ->
    maps:get(data_retention, get()).

parse(<<"data_retention">>, Str) ->
    {data_retention, to_duration_ms(Str)};
parse(<<"dispatcher_pool_size">>, Size) ->
    (Size < 1 orelse Size > 10240) andalso throw("invalid_dispatcher_pool_size"),
    {dispatcher_pool_size, Size}.

to_duration_ms(Str) ->
    case hocon_postprocess:duration(Str) of
        D when is_number(D) ->
            ceiling(D);
        _ ->
            case to_integer(Str) of
                I when is_integer(I) -> I;
                _ -> throw("invalid_duration_for_data_retention")
            end
    end.

ceiling(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T;
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.

to_integer(Str) ->
    case string:to_integer(Str) of
        {Int, <<>>} -> Int;
        _ -> error
    end.
