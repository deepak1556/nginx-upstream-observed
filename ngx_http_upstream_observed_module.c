#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

typedef struct {
  ngx_uint_t                           nreq;
  ngx_uint_t                           total_req;
  ngx_unit_t                           last_req_id;
  ngx_uint_t                           fails;
  ngx_uint_t                           current_weight;
}ngx_http_upstream_observed_shared_t;

typedef struct ngx_http_upstream_observed_peers_s ngx_http_upstream_observed_peers_t;

typedef struct {
  ngx_rbtree_node_t                    node;
  ngx_uint_t                           generation;
  uintptr_t                            peers; /*form cookie*/
  ngx_uint_t                           total_nreq;
  ngx_uint_t                           total_requests;
  ngx_atomic_t                         lock;
  ngx_http_upstream_observed_shared_t  stats[1];
}ngx_http_upstream_observed_shm_block_t;

#define ngx_spinlock_unlock(lock)      (void) ngx_atomic_cmp_set(lock, ngx_pid, 0);

typedef struct {
  ngx_http_upstream_observed_shared_t  *shared;
  struct sockaddr                      *sockaddr;
  socklen_t                             socklen;
  ngx_str_t                             name;

  ngx_uint_t                            weight;
  ngx_uint_t                            max_fails;
  time_t                                fail_timeout;
  
  time_t                                accessed;
  ngx_uint_t                            down:1;

#if (NGX_HTTP_SSL)
  ngx_ssl_session_t                    *ssl_session; /*based on process */
#endif
}ngx_http_upstream_observed_peer_t;

#define NGX_HTTP_UPSTREAM_OBSERVED_NO_RR            (1<<26)
#define NGX_HTTP_UPSTREAM_OBSERVED_WEIGHT_MODE_IDLE (1<<27)
#define NGX_HTTP_UPSTREAM_OBSERVED_WEIGHT_MODE_PEAK (1<<28)
#define NGX_HTTP_UPSTREAM_OBSERVED_WEIGHT_MODE_MASK ((1<<27) | (1<<28))

enum {WM_DEFAULT = 0, WM_IDLE, WM_PEAK};

struct ngx_http_upstream_observed_peer_s {
  ngx_http_upstream_observed_shm_block_t *shared;
  ngx_uint_t                              current;
  ngx_uint_t                              size_err:1;
  ngx_uint_t                              no_rr:1;
  ngx_uint_t                              weight_mode:2;
  ngx_uint_t                              number;
  ngx_str_t                              *name;
  ngx_http_upstream_observed_peers_t     *next; /*peers backup*/
  ngx_http_upstream_observed_peer_t       peer[1];
};

#define NGX_PEER_INVALID (~0UL)

typedef struct {
  ngx_http_upstream_observed_peers_t     *peers;
  ngx_uint_t                              current;
  uintptr_t                              *tried;
  uintptr_t                              *done;
  uintptr_t                               data;
  uintptr_t                               data2;
}ngx_http_upstream_observed_peer_data_t;

static ngx_int_t ngx_http_upstream_init_observed(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us);
static ngx_int_t ngx_http_upstream_get_observed_peer(ngx_peer_connection_t *pc, void *data);
static void ngx_http_upstream_free_observed_peer(ngx_peer_connection_t *pc, void *data, ngx_uint_t state);
static ngx_int_t ngx_http_upstream_init_observed_peer(ngx_http_request_t *r, ngx_http_upstream_srv_conf_t *us);
static char *ngx_http_upstream_observed(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_upstream_observed_set_shm_size(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static ngx_int_t ngx_http_upstream_observed_init_module(ngx_cycle_t *cycle);

#if (NGX_HTTP_SSL)
static ngx_int_t ngx_http_upstream_observed_set_session(ngx_peer_connection_t *pc, void *data);
static void ngx_http_upstream_observed_save_session(ngx_peer_connection_t *pc, void *data);
#endif

static ngx_comman_t ngx_http_upstream_observed_commands[] = {

  {ngx_string("observed"),
   NGX_HTTP_UPS_CONF | NGX_CONF_ANY,
   ngx_http_upstream_observed,
   0,
   0,
   NULL},

  {ngx_string("upstream_observed_shm_size"),
   NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1,
   ngx_http_upstream_observed_set_shm_size,
   0,
   0,
   NULL},

  ngx_null_command
};

static ngx_http_module_t ngx_http_upstream_observed_module_ctx = {
  NULL,           /*preconfiguration*/
  NULL,           /*postconfiguration*/

  NULL,           /*create main configuration*/
  NULL,           /*init main configuration*/

  NULL,           /*create server configuration*/
  NULL,           /*merge server configuration*/

  NULL,           /*create locaion configuration*/
  NULL,           /*merge location configuration*/

#if (NGX_HTTP_EXTENDED_STATUS)
  ngx_http_upstream_observed_report_status,
#endif
};

ngx_module_t ngx_http_upstream_observed_module = {
  NGX_MODULE_V1,
  &ngx_http_upstream_observed_module_ctx,
  ngx_http_upstream_observed_commands,
  NGX_HTTP_MODULE,
  NULL,
  ngx_http_upstream_observed_init_module,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NGX_MODULE_V1_PADDING
};

static ngx_uint_t ngx_http_upstream_observed_shm_size;
static ngx_shm_zone_t *ngx_http_upstream_observed_shm_zone
static ngx_rbtree_t *ngx_http_upstream_observed_rbtree;
static ngx_uint_t ngx_http_upstream_observed_generation; 
