#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <arpa/inet.h>

#include <ei.h>

#include <dqlite.h>

#define CHECK_DQLITE_VERSION(v) (DQLITE_VERSION_NUMBER >= (v))

struct state {
    dqlite_node* node;
    int started;
};

static int read_exact(size_t n, char** buf, size_t* buf_size)
{
    if (n > SSIZE_MAX) {
        return -1;
    }
    if (*buf_size < n) {
        char* tmp = realloc(*buf, n);
        if (tmp == NULL) {
            return -1;
        }
        *buf = tmp;
        *buf_size = n;
    }
    size_t off = 0;
    while (off < n) {
        ssize_t rd = read(0, *buf + off, n - off);
        if (rd < 0 || (rd == 0 && off != 0)) {
            return -1;
        }
        if (rd == 0 && off == 0) {
            return 1;
        }
        off += rd;
    }
    return 0;
}

static int write_exact(const char* buf, size_t n) {
    if (n > SSIZE_MAX) {
        return -1;
    }
    size_t off = 0;
    while (off < n) {
        ssize_t wr = write(1, buf + off, n - off);
        if (wr < 0) {
            return -1;
        }
        off += wr;
    }
    return 0;
}

static char* decode_string(const char* buf, int* index)
{
    int type, size;
    if (ei_get_type(buf, index, &type, &size) != 0) {
        return NULL;
    }
    if (type != ERL_STRING_EXT && type != ERL_BINARY_EXT) {
        return NULL;
    }
    char* s = malloc(size + 1);
    if (s == NULL) {
        return NULL;
    }
    if (type == ERL_STRING_EXT) {
        if (ei_decode_string(buf, index, s) != 0) {
            free(s);
            return NULL;
        }
    } else {
        long len;
        if (ei_decode_binary(buf, index, s, &len) != 0) {
            free(s);
            return NULL;
        }
        s[len] = '\0';
    }
    return s;
}

typedef int (*decode_prop_value)(const char* key, const char* buf, int* index, void* dec_ctx);

static int decode_proplist(const char* buf, int* index, decode_prop_value dec, void* dec_ctx)
{
    int arity;
    if (ei_decode_list_header(buf, index, &arity) != 0) {
        return -1;
    }
    for (int i = 0; i < arity; i++) {
        int arity;
        if (ei_decode_tuple_header(buf, index, &arity) != 0) {
            return -1;
        }
        if (arity != 2) {
            return -1;
        }
        char key[MAXATOMLEN];
        if (ei_decode_atom(buf, index, key) != 0) {
            return -1;
        }
        if (dec(key, buf, index, dec_ctx) != 0) {
            return -1;
        }
    }
    return 0;
}

static int decode_cmd_name(const char* buf, int* index, char* cmd)
{
    int type, size;
    if (ei_get_type(buf, index, &type, &size) != 0) {
        return -1;
    }
    switch (type) {
    case ERL_ATOM_EXT:
        break;
    case ERL_SMALL_TUPLE_EXT:
    case ERL_LARGE_TUPLE_EXT:
        {
            int arity;
            if (ei_decode_tuple_header(buf, index, &arity) != 0) {
                return -1;
            }
            if (arity != 2) {
                return -1;
            }
        }
        break;
    default:
        return -1;
    }
    if (ei_decode_atom(buf, index, cmd) != 0) {
        return -1;
    }
    return 0;
}

static int decode_node_info_ext(const char* buf, int* index, dqlite_node_info_ext* info)
{
    int arity;
    if (ei_decode_tuple_header(buf, index, &arity) != 0) {
        goto err;
    }
    if (arity != 4) {
        goto err;
    }
    char atom[MAXATOMLEN];
    if (ei_decode_atom(buf, index, atom) != 0) {
        goto err;
    }
    if (strcmp(atom, "node_info") != 0) {
        goto err;
    }
    unsigned long long id;
    if (ei_decode_ulonglong(buf, index, &id) != 0 || id > UINT64_MAX) {
        goto err;
    }
    info->id = (uint64_t)id;
    info->address = (uint64_t)decode_string(buf, index);
    if ((char*)info->address == NULL) {
        goto err;
    }
    if (ei_decode_atom(buf, index, atom) != 0) {
        goto err_free_addr;
    }
    if (strcmp(atom, "voter") == 0) {
        info->dqlite_role = 0;
    } else if (strcmp(atom, "standby") == 0) {
        info->dqlite_role = 1;
    } else if (strcmp(atom, "spare") == 0) {
        info->dqlite_role = 2;
    } else {
        goto err_free_addr;
    }
    info->size = DQLITE_NODE_INFO_EXT_SZ_ORIG;
    return 0;
 err_free_addr:
    free((void*)info->address);
 err:
    return -1;
}

struct create_cmd_params {
    int id_set;
    dqlite_node_id id;
    char* addr;
    char* dir;
    char* bind_addr;
    int network_latency_set;
    unsigned long long network_latency;
    int failure_domain_set;
    unsigned long long failure_domain;
#if CHECK_DQLITE_VERSION(10800)
    struct {
        int set;
        unsigned long threshold;
        unsigned long trailing;
    } snapshot_params;
#endif
#if CHECK_DQLITE_VERSION(11300)
    int disk_mode;
#endif
};

static int decode_create_cmd_param(const char* key, const char* buf, int* index, void* dec_ctx)
{
    struct create_cmd_params* p = dec_ctx;
    if (strcmp(key, "id") == 0) {
        if (ei_decode_ulonglong(buf, index, &p->id) != 0) {
            return -1;
        }
        p->id_set = 1;
    } else if (strcmp(key, "addr") == 0) {
        p->addr = decode_string(buf, index);
        if (p->addr == NULL) {
            return -1;
        }
    } else if (strcmp(key, "dir") == 0) {
        p->dir = decode_string(buf, index);
        if (p->dir == NULL) {
            return -1;
        }
    } else if (strcmp(key, "bind_addr") == 0) {
        p->bind_addr = decode_string(buf, index);
        if (p->bind_addr == NULL) {
            return -1;
        }
    } else if (strcmp(key, "network_latency") == 0) {
        if (ei_decode_ulonglong(buf, index, &p->network_latency) != 0) {
            return -1;
        }
        p->network_latency_set = 1;
    } else if (strcmp(key, "failure_domain") == 0) {
        if (ei_decode_ulonglong(buf, index, &p->failure_domain) != 0) {
            return -1;
        }
        p->failure_domain_set = 1;
#if CHECK_DQLITE_VERSION(10800)
    } else if (strcmp(key, "snapshot_params") == 0) {
        int arity;
        if (ei_decode_tuple_header(buf, index, &arity) != 0) {
            return -1;
        }
        if (arity != 2) {
            return -1;
        }
        if (ei_decode_ulong(buf, index, &p->snapshot_params.threshold) != 0) {
            return -1;
        }
        if (ei_decode_ulong(buf, index, &p->snapshot_params.trailing) != 0) {
            return -1;
        }
        p->snapshot_params.set = 1;
#endif
#if CHECK_DQLITE_VERSION(11300)
    } else if (strcmp(key, "disk_mode") == 0) {
        if (ei_decode_boolean(buf, index, &p->disk_mode) != 0) {
            return -1;
        }
#endif
    } else {
        return -1;
    }
    return 0;
}

typedef enum exec_result {
    EXEC_OK,
    EXEC_STOP,
    EXEC_UNKNOWN_CMD_ERROR,
    EXEC_DECODE_ERROR,
    EXEC_DQLITE_ERROR,
    EXEC_INVALID_STATE_ERROR,
    EXEC_MISSING_PARAM_ERROR,
    EXEC_SYSTEM_ERROR,
} exec_result_t;

static int write_exec_response(const struct state* st, exec_result_t res)
{
    int ret = 0;
    ei_x_buff x;
    if (ei_x_new_with_version(&x) != 0) {
        return -1;
    }
    switch (res) {
    case EXEC_OK:
    case EXEC_STOP:
        if (ei_x_encode_atom(&x, "ok") != 0) {
            goto err;
        }
        break;
    default:
        if (ei_x_encode_tuple_header(&x, 2) != 0) {
            goto err;
        }
        if (ei_x_encode_atom(&x, "error") != 0) {
            goto err;
        }
    }
    switch (res) {
    case EXEC_OK:
    case EXEC_STOP:
        break;
    case EXEC_UNKNOWN_CMD_ERROR:
        if (ei_x_encode_atom(&x, "unknown_cmd") != 0) {
            goto err;
        }
        break;
    case EXEC_DECODE_ERROR:
        if (ei_x_encode_atom(&x, "decode_error") != 0) {
            goto err;
        }
        break;
    case EXEC_DQLITE_ERROR:
        if (ei_x_encode_tuple_header(&x, 2) != 0) {
            goto err;
        }
        if (ei_x_encode_atom(&x, "dqlite_error") != 0) {
            goto err;
        }
        const char* msg = st->node != NULL ? dqlite_node_errmsg(st->node) : "";
        if (ei_x_encode_string(&x, msg) != 0) {
            goto err;
        }
        break;
    case EXEC_INVALID_STATE_ERROR:
        if (ei_x_encode_atom(&x, "invalid_state") != 0) {
            goto err;
        }
        break;
    case EXEC_MISSING_PARAM_ERROR:
        if (ei_x_encode_atom(&x, "missing_param") != 0) {
            goto err;
        }
        break;
    case EXEC_SYSTEM_ERROR:
        if (ei_x_encode_atom(&x, "system_error") != 0) {
            goto err;
        }
    default:
        goto err;
    }
    uint32_t term_size = htonl((uint32_t)x.index);
    if (write_exact((char*)&term_size, 4) != 0) {
        goto err;
    }
    if (write_exact(x.buff, x.index) != 0) {
        goto err;
    }
    goto out;
 err:
    ret = -1;
 out:
    ei_x_free(&x);
    return ret;
}

static exec_result_t execute_create_cmd(struct state* st, const char* buf, int* index)
{
    exec_result_t ret = EXEC_OK;
    struct create_cmd_params p = {
        .id_set = 0,
        .addr = NULL,
        .dir = NULL,
        .bind_addr = NULL,
        .network_latency_set = 0,
        .failure_domain_set = 0,
#if CHECK_DQLITE_VERSION(10800)
        .snapshot_params = {
            .set = 0,
        },
#endif
#if CHECK_DQLITE_VERSION(11300)
        .disk_mode = 0,
#endif
    };
    if (st->node != NULL) {
        ret = EXEC_INVALID_STATE_ERROR;
        goto out;
    }
    if (decode_proplist(buf, index, decode_create_cmd_param, &p) != 0) {
        ret = EXEC_DECODE_ERROR;
        goto out;
    }
    if (!p.id_set || p.addr == NULL || p.dir == NULL) {
        ret = EXEC_MISSING_PARAM_ERROR;
        goto out;
    }
    dqlite_node_create(p.id, p.addr, p.dir, &st->node);
    if (st->node == NULL) {
        ret = EXEC_DQLITE_ERROR;
        goto out;
    }
    const char* bind_addr = p.bind_addr != NULL ? p.bind_addr : p.addr;
    if (dqlite_node_set_bind_address(st->node, bind_addr) != 0) {
        ret = EXEC_DQLITE_ERROR;
        goto out;
    }
    if (p.network_latency_set &&
#if CHECK_DQLITE_VERSION(10900)
        dqlite_node_set_network_latency_ms(st->node, p.network_latency)
#else
        dqlite_node_set_network_latency(st->node, p.network_latency)
#endif
        != 0) {
        ret = EXEC_DQLITE_ERROR;
        goto out;
    }
    if (p.failure_domain_set && dqlite_node_set_failure_domain(st->node, p.failure_domain) != 0) {
        ret = EXEC_DQLITE_ERROR;
        goto out;
    }
#if CHECK_DQLITE_VERSION(10800)
    if (p.snapshot_params.set && dqlite_node_set_snapshot_params(st->node, p.snapshot_params.threshold, p.snapshot_params.trailing) != 0) {
        ret = EXEC_DQLITE_ERROR;
        goto out;
    }
#endif
#if CHECK_DQLITE_VERSION(11300)
    if (p.disk_mode && dqlite_node_enable_disk_mode(st->node) != 0) {
        ret = EXEC_DQLITE_ERROR;
        goto out;
    }
#endif
 out:
    free(p.addr);
    free(p.dir);
    free(p.bind_addr);
    return ret;
}

static exec_result_t execute_start_cmd(struct state* st, const char* buf, int* index)
{
    (void)buf;
    (void)index;
    if (st->node == NULL) {
        return EXEC_INVALID_STATE_ERROR;
    }
    if (dqlite_node_start(st->node) != 0) {
        return EXEC_DQLITE_ERROR;
    }
    st->started = 1;
    return EXEC_OK;
}

static exec_result_t execute_stop_cmd(struct state* st, const char* buf, int* index)
{
    (void)buf;
    (void)index;
    if (st->node == NULL || !st->started) {
        return EXEC_INVALID_STATE_ERROR;
    }
    if (dqlite_node_stop(st->node) != 0) {
        return EXEC_DQLITE_ERROR;
    }
    st->started = 0;
    return EXEC_OK;
}

static exec_result_t execute_destroy_cmd(struct state* st, const char* buf, int* index)
{
    (void)buf;
    (void)index;
    if (st->node == NULL || st->started) {
        return EXEC_INVALID_STATE_ERROR;
    }
    dqlite_node_destroy(st->node);
    st->node = NULL;
    return EXEC_STOP;
}

static exec_result_t execute_recover_cmd(struct state* st, const char* buf, int* index)
{
    exec_result_t ret = EXEC_OK;
    int cluster_size = 0;
    dqlite_node_info_ext* cluster = NULL;
    if (st->node == NULL || st->started) {
        ret = EXEC_INVALID_STATE_ERROR;
        goto out;
    }
    if (ei_decode_list_header(buf, index, &cluster_size) != 0) {
        ret = EXEC_DECODE_ERROR;
        goto out;
    }
    cluster = malloc(cluster_size * sizeof(*cluster));
    if (cluster == NULL) {
        ret = EXEC_SYSTEM_ERROR;
        goto out;
    }
    for (int i = 0; i < cluster_size; i++) {
        if (decode_node_info_ext(buf, index, &cluster[i]) != 0) {
            ret = EXEC_DECODE_ERROR;
            goto out;
        }
    }
    if (dqlite_node_recover_ext(st->node, cluster, cluster_size) != 0) {
        ret = EXEC_DQLITE_ERROR;
        goto out;
    }
 out:
    if (cluster != NULL) {
        for (int i = 0; i < cluster_size; i++) {
            free((void*)cluster[i].address);
        }
        free(cluster);
    }
    return ret;
}

static exec_result_t execute_cmd(struct state* st, const char* cmd, const char* buf, int* index)
{
    if (strcmp(cmd, "create") == 0) {
        return execute_create_cmd(st, buf, index);
    } else if (strcmp(cmd, "start") == 0) {
        return execute_start_cmd(st, buf, index);
    } else if (strcmp(cmd, "stop") == 0) {
        return execute_stop_cmd(st, buf, index);
    } else if (strcmp(cmd, "destroy") == 0) {
        return execute_destroy_cmd(st, buf, index);
    } else if (strcmp(cmd, "recover") == 0) {
        return execute_recover_cmd(st, buf, index);
    } else {
        return EXEC_UNKNOWN_CMD_ERROR;
    }
}

typedef enum process_result {
    PROCESS_CONTINUE,
    PROCESS_ERROR,
    PROCESS_STOP
} process_result_t;

static process_result_t process_msg(struct state* st, char** buf, size_t* buf_size)
{
    switch (read_exact(4, buf, buf_size)) {
    case 1:
        return PROCESS_STOP;
    case -1:
        return PROCESS_ERROR;
    }
    uint32_t term_size = ntohl(*(uint32_t*)*buf);
    switch (read_exact(term_size, buf, buf_size)) {
    case 1:
        return PROCESS_STOP;
    case -1:
        return PROCESS_ERROR;
    }
    int index = 0;
    int version;
    if (ei_decode_version(*buf, &index, &version) != 0) {
        return PROCESS_ERROR ;
    }
    char cmd[MAXATOMLEN];
    if (decode_cmd_name(*buf, &index, cmd) != 0) {
        return PROCESS_ERROR;
    }
    exec_result_t res = execute_cmd(st, cmd, *buf, &index);
    if (write_exec_response(st, res) != 0) {
        return PROCESS_ERROR;
    }
    switch (res) {
    case EXEC_STOP:
        return PROCESS_STOP;
    case EXEC_DECODE_ERROR:
        return PROCESS_ERROR;
    default:
        return PROCESS_CONTINUE;
    }
}

int main(void)
{
    int ret = 0;
    char* buf = NULL;
    size_t buf_size = 0;
    struct state st = {
        .node = NULL,
        .started = 0,
    };
    if (ei_init() != 0) {
        goto err;
    }
    for (;;) {
        switch (process_msg(&st, &buf, &buf_size)) {
        case PROCESS_CONTINUE:
            continue;
        case PROCESS_ERROR:
            goto err;
        case PROCESS_STOP:
            goto out;
        }
    }
 err:
    ret = 1;
 out:
    free(buf);
    if (st.node != NULL) {
        if (st.started) {
            dqlite_node_stop(st.node);
        }
        dqlite_node_destroy(st.node);
    }
    return ret;
}
