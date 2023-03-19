-module(dqliterl_proto).

-dialyzer(no_improper_lists).

-export([
    encode_req/1,
    decode_resp/1
]).

-export_type([
    node_id/0,
    node_role/0,
    node_info/0
]).

-include("dqliterl.hrl").

-type uint8() :: 0..255.
-type uint64() :: 0..18446744073709551615.
-type int64() :: -9223372036854775808..9223372036854775807.
-type uint32() :: 0..4294967295.

-type node_id() :: uint64().
-type node_role() :: voter | standby | spare.
-type node_info() :: #node_info{}.

-type schema_version() :: uint8().
-type client_id() :: uint64().
-type database_id() :: uint32().
-type stmt_id() :: uint32().
-type failure_domain() :: uint64().
-type weight() :: uint64().
-type param_tuple() :: [param_value()].
-type param_value() ::
    {int, int64()}
    | {float, float()}
    | {text, unicode:chardata()}
    | {blob, binary()}
    | null
    | {date, unicode:chardata()}
    | {bool, boolean()}.
-type req_schema_type() :: uint8().
-type req() ::
    leader
    | {client, client_id()}
    | {open, unicode:chardata()}
    | {prepare, database_id(), unicode:chardata()}
    | {exec, database_id(), stmt_id(), param_tuple()}
    | {query, database_id(), stmt_id(), param_tuple()}
    | {finalize, database_id(), stmt_id()}
    | {exec_sql, database_id(), unicode:chardata(), param_tuple()}
    | {query_sql, database_id(), unicode:chardata(), param_tuple()}
    | {interrupt, database_id()}
    | {add, #node_info0{}}
    | {assign, node_id(), node_role()}
    | {remove, node_id()}
    | {dump, unicode:chardata()}
    | cluster
    | {cluster, uint64()}
    | {leadership, node_id()}.
-type error_code() :: uint64().
-type error_message() :: unicode:chardata().
-type resp_schema_type() :: uint8().
-type resp() ::
    {failure, error_code(), error_message()}
    | {leader, #node_info0{}}
    | welcome
    | {cluster, [#node_info{}]}
    | {database, database_id()}
    | {stmt, database_id(), stmt_id(), non_neg_integer()}
    | {exec, non_neg_integer(), non_neg_integer()}
    | ack
    | {files, [#file{}]}
    | {metadata, failure_domain(), weight()}.

-spec encode_req(req()) -> iodata().
encode_req(leader) ->
    add_header(0, encode({uint64, 0}));
encode_req({client, ClientId}) ->
    add_header(1, encode({uint64, ClientId}));
encode_req({open, DbName}) ->
    add_header(3, [encode({text, DbName}), encode({uint64, 0}) | encode({text, ""})]);
encode_req({prepare, DbId, Sql}) ->
    add_header(4, [encode({uint64, DbId}) | encode({text, Sql})]);
encode_req({exec, DbId, StmtId, Params}) ->
    {Schema, EncodedParams} = encode_param_tuple(Params),
    add_header(5, Schema, [encode({uint32, DbId}), encode({uint32, StmtId}) | EncodedParams]);
encode_req({query, DbId, StmtId, Params}) ->
    {Schema, EncodedParams} = encode_param_tuple(Params),
    add_header(6, Schema, [encode({uint32, DbId}), encode({uint32, StmtId}) | EncodedParams]);
encode_req({finalize, DbId, StmtId}) ->
    add_header(7, [encode({uint32, DbId}) | encode({uint32, StmtId})]);
encode_req({exec_sql, DbId, Sql, Params}) ->
    {Schema, EncodedParams} = encode_param_tuple(Params),
    add_header(8, Schema, [encode({uint64, DbId}), encode({text, Sql}) | EncodedParams]);
encode_req({query_sql, DbId, Sql, Params}) ->
    {Schema, EncodedParams} = encode_param_tuple(Params),
    add_header(9, Schema, [encode({uint64, DbId}), encode({text, Sql}) | EncodedParams]);
encode_req({interrupt, DbId}) ->
    add_header(10, encode({uint64, DbId}));
encode_req({add, Info0}) ->
    add_header(12, encode(Info0));
encode_req({assign, NodeId, Role}) ->
    add_header(13, [encode({uint64, NodeId}) | encode({node_role, Role})]);
encode_req({remove, NodeId}) ->
    add_header(14, encode({uint64, NodeId}));
encode_req({dump, Name}) ->
    add_header(15, encode({text, Name}));
encode_req(cluster) ->
    encode_req({cluster, 1});
encode_req({cluster, Format}) ->
    add_header(16, encode({uint64, Format}));
encode_req({leadership, NodeId}) ->
    add_header(17, encode({uint64, NodeId})).

-spec add_header(req_schema_type(), iodata()) -> iodata().
add_header(Type, Body) ->
    add_header(Type, 0, Body).

-spec add_header(req_schema_type(), schema_version(), iodata()) -> iodata().
add_header(Type, Schema, Body) ->
    Len = erlang:iolist_size(Body) div 8,
    [<<Len:32/little, Type, Schema, 0, 0>> | Body].

-spec encode_param_tuple(param_tuple()) -> {schema_version(), iodata()}.
encode_param_tuple(Params) ->
    {Schema, Header} = encode_param_tuple_header(Params),
    {Schema, [Header | encode_param_tuple_values(Params)]}.

-spec encode_param_tuple_header(param_tuple()) -> {schema_version(), iodata()}.
encode_param_tuple_header(Params) ->
    encode_param_tuple_header(Params, 0, []).

-spec encode_param_tuple_header(param_tuple(), uint32(), iodata()) ->
    {schema_version(), iodata()}.
encode_param_tuple_header([], N, Header) ->
    Header2 = [encode_param_tuple_size(N) | Header],
    Schema =
        if
            N > 255 -> 1;
            true -> 0
        end,
    {Schema, [Header2 | padding(iolist_size(Header2))]};
encode_param_tuple_header([Param | Rest], N, Header) ->
    encode_param_tuple_header(Rest, N + 1, [Header | encode_param_value_type(Param)]).

-spec encode_param_tuple_values(param_tuple()) -> iodata().
encode_param_tuple_values(Params) ->
    encode_param_tuple_values(Params, []).

-spec encode_param_tuple_values(param_tuple(), iodata()) -> iodata().
encode_param_tuple_values([], Acc) ->
    Acc;
encode_param_tuple_values([Param | Rest], Acc) ->
    encode_param_tuple_values(Rest, [Acc | encode_param_value(Param)]).

-spec encode_param_tuple_size(uint32()) -> binary().
encode_param_tuple_size(N) when N > 255 ->
    <<N:32/little>>;
encode_param_tuple_size(N) ->
    <<N>>.

-spec encode_param_value_type(param_value()) -> binary().
encode_param_value_type({int, _}) ->
    <<1>>;
encode_param_value_type({float, _}) ->
    <<2>>;
encode_param_value_type({text, _}) ->
    <<3>>;
encode_param_value_type({blob, _}) ->
    <<4>>;
encode_param_value_type(null) ->
    <<5>>;
encode_param_value_type({date, _}) ->
    <<10>>;
encode_param_value_type({bool, _}) ->
    <<11>>.

-spec encode_param_value(param_value()) -> iodata().
encode_param_value({int, I}) ->
    encode({int64, I});
encode_param_value({float, F}) ->
    encode({float, F});
encode_param_value({text, T}) ->
    encode({text, T});
encode_param_value({blob, B}) ->
    encode({blob, B});
encode_param_value(null) ->
    encode(null);
encode_param_value({date, D}) ->
    encode({date, D});
encode_param_value({bool, B}) ->
    encode({bool, B}).

-spec encode
    ({uint64, uint64()}) -> binary();
    ({int64, int64()}) -> binary();
    ({uint32, uint32()}) -> binary();
    ({float, float()}) -> binary();
    ({text, unicode:chardata()}) -> iodata();
    ({blob, binary()}) -> iodata();
    ({bool, boolean()}) -> binary();
    (null) -> binary();
    ({date, unicode:chardata()}) -> binary();
    ({node_role, node_role()}) -> binary();
    (#node_info0{}) -> iodata().
encode({uint64, N}) ->
    <<N:64/little>>;
encode({int64, N}) ->
    <<N:64/signed-little>>;
encode({uint32, N}) ->
    <<N:32/little>>;
encode({float, F}) ->
    <<F/float-little>>;
encode({text, S}) ->
    Data = unicode:characters_to_binary(S),
    [Data, 0 | padding(erlang:size(Data) + 1)];
encode({blob, B}) ->
    Len = erlang:size(B),
    [encode({uint64, Len}), B | padding(Len)];
encode({bool, false}) ->
    encode({uint64, 0});
encode({bool, true}) ->
    encode({uint64, 1});
encode(null) ->
    encode({uint64, 0});
encode({date, Date}) ->
    encode({text, Date});
encode({node_role, voter}) ->
    encode({uint64, 0});
encode({node_role, standby}) ->
    encode({uint64, 1});
encode({node_role, spare}) ->
    encode({uint64, 2});
encode(#node_info0{id = Id, address = Addr}) ->
    [encode({uint64, Id}) | encode({text, Addr})].

-spec decode_resp(binary()) -> {ok, resp(), binary()} | {more, pos_integer()}.
decode_resp(Bin) ->
    case Bin of
        <<L:32/little, Type, Schema, _, _, Body:(L * 8)/binary, Rest/binary>> ->
            {ok, decode_resp(Type, Schema, Body), Rest};
        <<L:32/little, Rest/binary>> ->
            {more, L * 8 + 4 - erlang:size(Rest)};
        _ ->
            {more, 8 - erlang:size(Bin)}
    end.

-spec decode_resp(resp_schema_type(), schema_version(), binary()) -> resp().
decode_resp(0, _Schema, Body) ->
    {Code, Rest} = decode(uint64, Body),
    {Message, <<>>} = decode(text, Rest),
    {failure, Code, Message};
decode_resp(1, _Schema, Body) ->
    {Info0, <<>>} = decode(node_info0, Body),
    {leader, Info0};
decode_resp(2, _Schema, Body) ->
    {_, <<>>} = decode(uint64, Body),
    welcome;
decode_resp(3, _Schema, Body) ->
    {N, Rest} = decode(uint64, Body),
    {Infos, <<>>} = decode_array(N, node_info, Rest),
    {cluster, Infos};
decode_resp(4, _Schema, Body) ->
    {Id, Rest} = decode(uint32, Body),
    {_, <<>>} = decode(uint32, Rest),
    {database, Id};
decode_resp(5, _Schema, Body) ->
    {DbId, Rest} = decode(uint32, Body),
    {StmtId, Rest2} = decode(uint32, Rest),
    {N, <<>>} = decode(uint64, Rest2),
    {stmt, DbId, StmtId, N};
decode_resp(6, _Schema, Body) ->
    {RowId, Rest} = decode(uint64, Body),
    {N, <<>>} = decode(uint64, Rest),
    {exec, RowId, N};
decode_resp(8, _Schema, Body) ->
    {_, <<>>} = decode(uint64, Body),
    ack;
decode_resp(9, _Schema, Body) ->
    {N, Rest} = decode(uint64, Body),
    {Files, <<>>} = decode_array(N, file, Rest),
    {files, Files};
decode_resp(10, _Schema, Body) ->
    {Domain, Rest} = decode(uint64, Body),
    {Weight, <<>>} = decode(uint64, Rest),
    {metadata, Domain, Weight}.

decode_array(N, Type, Bin) ->
    decode_array(N, Type, Bin, []).

decode_array(0, _Type, Bin, Acc) ->
    {lists:reverse(Acc), Bin};
decode_array(N, Type, Bin, Acc) ->
    {Elem, Rest} = decode(Type, Bin),
    decode_array(N - 1, Type, Rest, [Elem | Acc]).

-spec decode
    (uint64, binary()) -> {uint64(), binary()};
    (int64, binary()) -> {int64(), binary()};
    (uint32, binary()) -> {uint32(), binary()};
    (float, binary()) -> {float(), binary()};
    (text, binary()) -> {unicode:chardata(), binary()};
    (blob, binary()) -> {binary(), binary()};
    (bool, binary()) -> {boolean(), binary()};
    (null, binary()) -> {null, binary()};
    (date, binary()) -> {unicode:chardata(), binary()};
    (node_role, binary()) -> {node_role(), binary()};
    (node_info0, binary()) -> {#node_info0{}, binary()};
    (node_info, binary()) -> {#node_info{}, binary()};
    (file, binary()) -> {#file{}, binary()}.
decode(uint64, <<N:64/little, Rest/binary>>) ->
    {N, Rest};
decode(int64, <<I:64/little, Rest/binary>>) ->
    {I, Rest};
decode(uint32, <<N:32/little, Rest/binary>>) ->
    {N, Rest};
decode(float, <<F/float-little, Rest/binary>>) ->
    {F, Rest};
decode(text, Bin) ->
    [S, Rest] = string:split(Bin, [$\0]),
    PaddingLen = compute_padding_len(erlang:size(S) + 1),
    <<0:PaddingLen/unit:8, Rest2/binary>> = Rest,
    {S, Rest2};
decode(blob, Bin) ->
    {L, Rest} = decode(uint64, Bin),
    <<B:L/binary, Rest2/binary>> = Rest,
    PaddingLen = compute_padding_len(erlang:size(B)),
    <<0:PaddingLen/unit:8, Rest3/binary>> = Rest2,
    {B, Rest3};
decode(bool, Bin) ->
    case decode(uint64, Bin) of
        {0, Rest} ->
            {false, Rest};
        {1, Rest} ->
            {true, Rest}
    end;
decode(null, Bin) ->
    {0, Rest} = decode(uint64, Bin),
    {null, Rest};
decode(date, Bin) ->
    decode(text, Bin);
decode(node_role, Bin) ->
    case decode(uint64, Bin) of
        {0, Rest} ->
            {voter, Rest};
        {1, Rest} ->
            {standby, Rest};
        {2, Rest} ->
            {spare, Rest}
    end;
decode(node_info0, Bin) ->
    {Id, Rest} = decode(uint64, Bin),
    {Addr, Rest2} = decode(text, Rest),
    {#node_info0{id = Id, address = Addr}, Rest2};
decode(node_info, Bin) ->
    {Id, Rest} = decode(uint64, Bin),
    {Addr, Rest2} = decode(text, Rest),
    {Role, Rest3} = decode(node_role, Rest2),
    {#node_info{id = Id, address = Addr, role = Role}, Rest3};
decode(file, Bin) ->
    {Name, Rest} = decode(text, Bin),
    {L, Rest2} = decode(uint64, Rest),
    <<B:L/binary, Rest3/binary>> = Rest2,
    {#file{name = Name, content = B}, Rest3}.

-spec padding(non_neg_integer()) -> binary().
padding(N) ->
    binary:copy(<<0>>, compute_padding_len(N)).

-spec compute_padding_len(non_neg_integer()) -> non_neg_integer().
compute_padding_len(N) ->
    case N rem 8 of
        0 -> 0;
        Rest -> 8 - Rest
    end.
