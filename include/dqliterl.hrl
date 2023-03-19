-record(node_info0, {
    id :: dqliterl_proto:node_id(),
    address :: unicode:chardata()
}).

-record(node_info, {
    id :: dqliterl_proto:node_id(),
    address :: unicode:chardata(),
    role :: dqliterl_proto:node_role()
}).

-record(file, {
    name :: unicode:chardata(),
    content :: binary()
}).
