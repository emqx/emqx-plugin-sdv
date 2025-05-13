-module(emqx_sdv_fanout_dispatcher).

-export([batch/1, ack/2]).

-include("emqx_sdv_fanout.hrl").

%% @doc Handle a batch received from SDV platform.
batch(Payload) ->
    try emqx_utils_json:decode(Payload, [return_maps]) of
        #{<<"ids">> := VINs, <<"request_id">> := RequestId, <<"data">> := Data} ->
            ok = insert_batch(VINs, RequestId, Data),
            ok = notify_workers(VINs, RequestId),
            ok;
        _ ->
            {error, invalid_payload}
    catch
        _:_ ->
            {error, invalid_payload}
    end.

%% @doc Handle an ACK received from a vehicle.
ack(VIN, RequestId) ->
    %% TODO: store the ACK, and send sync notification to dispatcher workers
    ?LOG(warning, "received_ack", #{vin => VIN, request_id => RequestId}),
    ok.

insert_batch(VINs, RequestId, Data) ->
    Now = now_ts(),
    {ok, ID} = emqx_sdv_fanout_data:insert(Now, Data),
    lists:foreach(fun(VIN) -> emqx_sdv_fanout_ids:insert(VIN, RequestId, Now, ID) end, VINs).

notify_workers(_VINs, _RequestId) ->
    %% TODO: send sync notification to dispatcher workers
    ok.

now_ts() ->
    erlang:system_time(millisecond).
