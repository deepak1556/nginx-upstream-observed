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

static int ngx_http_upstream_observed_compare_rbtree_node(const ngx_rbtree_node_t *v_left, const ngx_rbtree_node_t *v_right) {
  ngx_http_upstream_observed_shm_block_t *left, *right;

  left = (ngx_http_upstream_observed_shm_block_t *) v_left;
  right = (ngx_http_upstream_observed_shm_block_t *) v_right;

  if(left->generation < right->generation) {
    return -1;
  }else if(left->generation > right->generation){
    return 1;
  }else {
    if(left->peers < right->peers){
      return -1;
    }else if(left->peers > right>peers) {
      return 1;
    }else {
      return 0;
    }
  }
}

static void ngx_rbtree_generic_insert(ngx_rbtree_node_t *temp, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel, int (*compare)(const ngx_rbtree_node_t *left, const ngx_rbtree_node_t *right)) {
  for(;;) {
    if(node->key < temp->key) {
      if(temp->left == sentinel) {
	temp->left = node;
	break;
      }
      temp = temp->left;
    }else if(node->key > temp->key) {
      if(temp->right == sentinel) {
	temp->right = node;
	break;
      }
      temp = temp->right;
    }else {
      if(compare(node,temp) < 0) {
	if(temp->left == sentinel) {
	  temp->left = node;
	  break;
	}
	temp = temp->left;
      }else {
	if(temp->right == sentinel) {
	  temp->right = node;
	  break;
	}
	temp = temp->right;
      }
    }
  }
  node->parent = temp;
  node->left = sentinel;
  node->right = sentinel;
  ngx_rbt_red(node);
}

#define NGX_BIVECTOR_ELT_SIZE (sizeof(uintptr_t) * 8)

static uintptr_t *ngx_bivector_alloc(ngx_pool_t *pool, ngx_uint_t size, uintptr_t *small) {
  ngx_uint_t nelts = (size + NGX_BIVECTOR_ELT_SIZE - 1) / NGX_BIVECTOR_ELT_SIZE;

  if(small && nelts == 1) {
    *small = 0;
    return small;
  }

  return ngx_pcalloc(pool, nelts *NGX_BIVECTOR_ELT_SIZE);
}

static ngx_int_t ngx_bivector_test(uintptr_t *bv, ngx_uint_t bit) {
  ngx_uint_t n, m;

  n = bit / NGX_BIVECTOR_ELT_SIZE;
  m = 1 << (bit % NGX_BIVECTOR_ELT_SIZE);

  return bv[n] & m;
}

static void ngx_bivector_set(uintptr_t *bv, ngx_uint_t bit) {
  ngx_uint_t n, m;

  n = bit / NGX_BIVECTOR_ELT_SIZE:
  m = 1 << (bit % NGX_BIVECTOR_ELT_SIZE);

  bv[n] |= m;
}

static ngx_int_t ngx_http_upstream_init_module(ngx_cycle_t *cycle) {
  ngx_http_upstream_observed_generation++;
  return NGX_OK;
}

static void ngx_http_upstream_observed_rbtree_insert(ngx_rbtree_node_t *temp, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel) {
  ngx_rbtree_generic_insert(temp, node, sentinel, ngx_http_upstream_observed_compare_rbtree_node);
}

static ngx_int_t ngx_http_upstream_observed_init_shm_zone(ngx_shm_zone_t *shm_zone, void *data) {
  ngx_slab_pool_t   *shpool;
  ngx_rbtree_t      *tree;
  ngx_rbtree_node_t *sentinel;

  if(data) {
    shm_zone->data = data;
    return NGX_OK;
  }

  shpool = (ngx_slab_pool_t *) shm_zone->shm.addr;
  tree = ngx_slab_alloc(shpool, sizeof *tree);
  if(tree == NULL) {
    return NGX_ERROR;
  }

  sentinel = ngx_slab_alloc(shpool, sizeof *sentinel);
  if(sentinel == NULL) {
    return NGX_ERROR;
  }

  ngx_rbtree_sentinel_init(sentinel);
  tree->root = sentinel;
  tree->sentinel = sentinel;
  tree->insert = ngx_http_upstream_observed_rbtree_insert;
  shm_zone->data = tree;
  ngx_http_upstream_observed_rbtree = tree;

  return NGX_OK;
}

static char *ngx_http_upstream_observed_set_shm_size(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ssize_t   new_shm_size;
  ngx_str_t *value;

  value = cf->args->elts;
  
  new_shm_size = ngx_parse_size(&value[1]);
  if(new_shm_size == NGX_ERROR) {
    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "Invalid memory area size '%V'", &value[1]);
    return NGX_CONF_ERROR;
  }

  new_shm_size = ngx_align(new_shm_size, ngx_pagesize);

  if(new_shm_size < 8 * (ssize_t) ngx_pagesize) {
    ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "The upstream_observed_shm_size value must be atleast %udKiB", (8 * ngx_pagesize) >> 10);
  }

  if(ngx_http_upstream_observed_shm_size && ngx_http_upstream_observed_shm_size != (ngx_uint_t) new_shm_size) {
    ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "Cannot change memory area size without restart, ignoringchange");
  }else {
    ngx_http_upstream_observed_shm_size = new_shm_size;
  }

  ngx_conf_log_error(NGX_LOG_DEBUG, cf, 0, "Using %udKiB of shared memory for upstream_observed", new_shm_size >> 10);

  return NGX_CONF_OK;
}

static char *ngx_http_upstream_observed(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_http_upstream_srv_conf_t *uscf;
  ngx_uint_t i;
  ngx_uint_t extra_peer_flags = 0;

  for(i = 1; i < cf->args->nelts; i++) {
    ngx_str_t *value =  cf->args->elts;
    if(ngx_strcmp(value[i].data, "no_rr") == 0) {
      extra_peer_flags |= NGX_HTTP_UPSTREAM_OBSERVED_NO_RR;
    }else if(ngx_strcmp(value[i].data, "weight_mode=peak") == 0) {
      if(extra_peer_flags & NGX_HTTP_UPSTREAM_OBSERVED_WEIGHT_MODE_MASK) {
	ngx_conf_log_error(NGX_CONF_EMERG, cf, 0, "weight_mode = options are mutually exclusive");
	return NGX_CONF_ERROR;
      }
      extra_peer_flags |= NGX_HTTP_UPSTREAM_OBSERVED_WEIGHT_MODE_PEAK;
    }else if(ngx_strcmp(value[i].data, "weight_mode=idle") == 0) {
      ngx_conf_log_error(NGX_CONF_EMERG, cf, 0, "weight_mode = options are mutually exclusive");
      return NGX_CONF_ERROR;
    }
    extra_peer_flags |= NGX_HTTP_UPSTREAM_OBSERVED_WEIGHT_MODE_IDLE;
  }else {
    ngx_conf_log_error(NGX_CONF_EMERG, cf, 0, "Invalid observed parameter '%V'", &value[1]);
    return NGX_CONF_ERROR;
  }

  uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);
  
  uscf->peer.init_upstream = ngx_http_upstream_init_observed;

  uscf->flags = NGX_HTTP_UPSTREAM_CREATE | NGX_HTTP_UPSTREAM_WEIGHT | NGX_HTTP_UPSTREAM_MAX_FAILS | NGX_HTTP_UPSTREAM_FAIL_TIMEOUT | NGX_HTTP_UPSTREAM_DOWN | extra_peer_flags;

  return NGX_CONF_OK;
}

static ngx_int_t ngx_http_upstream_cmp_servers(const void *one, const void *two) {
  const ngx_http_upstream_observed_peer_t *first, *second;
  
  first = one;
  second = two;

  return (first->weight < second->weight);
}

static ngx_int_t ngx_http_upstream_init_observed(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us) {
  ngx_url_t                          u;
  ngx_uint_t                         i, j, n;
  ngx_http_upstream_server_t         *server;
  ngx_http_upstream_observed_peers_t *peers, *backup;

  if(us->servers) {
    server = us->servers->elts;

    n = 0;

    for(i = 0; i < us->servers->nelts; i++) {
      if(server[i].backup) {
	continue;
      }

      n += server[i].naddrs;
    }

    peers = ngx_palloc(cf->pool, sizeof(ngx_http_upstream_observed_peers_t) + sizeof(ngx_http_upstream_observed_peer_t) * (n - 1));

    if(peers == NULL) {
      return NGX_ERROR;
    }

    peers->number = n;
    peers->name = &u->host;

    n = 0;

    for(i = 0; i < us->servers->nelts; i++) {
      for(j = 0; j < server[i].naddrs; j++) {
	if(server[i].backup) {
	  continue;
	}

	peers->peer[n].sockaddr = server[i].addrs[j].sockaddr;
	peers->peer[n].socklen = server[i].addrs[j].socklen;
	peers->peer[n].name = server[i].addrs[j].name;
	peers->peer[n].max_fails  = server[i].max_fails;
	peers->peer[n].fail_timeout = server[i].fail_timeout;
	peers->peer[n].down = server[i].down;
	peers->peer[n].weight = server[i].down ? 0 : server[i].weight;
	n++;
      }
    }

    us->peer.data = peers;

    ngx_sort(&peers->peer[0], (size_t) n, sizeof(ngx_http_upstream_observed_peer_t), ngx_http_upstream_cmp_servers);

    n = 0;
    
    for(i = 0; i < us->servers->nelts; i++) {
      if(!server[i].backup) {
	continue;
      }

      n += server[i].naddrs;
    }

    if(n == 0) {
      return NGX_OK;
    }

    backup = ngx_palloc(cf->pool, sizeof(ngx_http_upstream_observed_peers_t) + sizeof(ngx_http_upstream_observed_peer_t) * (n - 1));

    if(backup == NULL) {
      return NGX_ERROR;
    }

    backup->number = n;
    backup->name = &us->host;

    n = 0;

    for(i = 0; i < us->servers->nelts; i++) {
      for(j = 0; j < server[i].naddrs; j++) {
	if(!server[i].backup) {
	  continue;
	}

	backup->peer[n].sockaddr = server[i].addrs[j].sockaddr;
	backup->peer[n].socklen = server[i].addrs[j].socklen;
	backup->peer[n].name = server[i].addrs[j].name;
	backup->peer[n].weight = server[i].weight;
	backup->peer[n].max_fails = server[i].max_fails;
	backup->peer[n].fail_timeout = server[i].fail_timeout;
	backup->peer[n].down = server[i].down;
	n++;
      }
    }

    peers->next = backup;

    ngx_sort(&backup->peer[0], (size_t) n, sizeof(ngx_http_upstream_observed_peer_t), ngx_http_upstream_cmp_servers);

    return NGX_OK;
  }

  if(us->port == 0 && us->default_port == 0) {
    ngx_log_error(NGX_LOG_EMERG, cf->log, 0, "no port in upstream \"%V\" in %s:%ui", &us->host, us->file_name, us_line);
    return NGX_ERROR;
  }

  ngx_memzero(&u, sizeof(ngx_url_t));

  u.host = us->host;
  u.port = (in_port_t) (us->port ? us->port : us->default_port);

  if(ngx_inet_resolve_host(cf_pool, &u) != NGX_OK) {
    if(u.err) {
      ngx_log_error(NGX_LOG_EMERG, cf->log, 0, "%s in upstream \"%V\" in %s:%ui", u.err, &us->host, us->file_name, us->line);
    }

    return NGX_ERROR;
  }

  n = u.addrs;

  peers = ngx_palloc(cf->pool, sizeof(ngx_http_upstream_observed_peer_t) + sizeof(ngx_http_upstream_observed_peer_t) * (n - 1));
  if(peers == NULL) {
    return NGX_ERROR;
  }

  peers->number = n;
  peers->name = &us->host;

  for(i = 0; i < u.naddrs; i++) {
    peers->peer[i].sockaddr = u.addrs[i].sockaddr;
    peers->peer[i].socklen = u.addrs[i].socklen;
    peers->peer[i].name = u.addrs[i].name;
    peers->peer[i].weight = 1;
    peers->peer[i].max_fails = 1;
    peers->peer[i].fail_timeout = 10;
  }

  us->peer.data = peers;

  return NGX_OK;
}

static ngx_int_t ngx_http_upstream_init_observed(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us) {
  ngx_http_upstream_observed_observed_peers_t *peers;
  ngx_uint_t                                   n;
  ngx_str_t                                   *shm_name;

  if(ngx_http_upstream_init_observed_rr(cf, us) != NGX_OK) {
    return NGX_ERROR:
  }

  peers = ngx_palloc(cf->pool, sizeof *peers);
  if(peers == NULL) {
    return NGX_ERROR;
  }

  peers = us->peer.data;
  n = peers->number;

  shm_name = ngx_palloc(cf->pool, sizeof *shm_name);
  shn_name->len = sizeof("upstream_observed") - 1;
  shm_name->data = (unsigned char *) "upstream_observed";

  if(ngx_http_upstream_observed_shm_size == 0) {
    ngx_http_upstream_observed_shm_size = 8 * ngx_pagesize;
  }

  ngx_http_upstream_observed_shm_zone = ngx_shared_memory_add(cf, shm_name, ngx_http_upstream_observed_shm_size, &ngx_http_upstream_observed_module);
  if(ngx_http_upstream_observed_shm_zone == NULL) {
    return NGX_ERROR;
  }

  ngx_http_upsdtream_observed_shm_zone->init = ngx_http_upstream_observed_init_shm_zone;

  peer->shared = NULL;
  peer->current = n - 1;
  if(us->flags & NGX_HTTP_UPSTREAM_OBSERVED_NO_RR) {
    peers->no_rr = 1;
  }
  if(us->flags & NGX_HTTP_UPSTREAM_OBSERVED_WEIGHT_MODE_IDLE) {
    peers->weight_mode = WM_IDLE;
  }else if(us->flags & NGX_HTTP_UPSTREAM_OBSERVED_WEIGHT_MODE_PEAK) {
    peers->weight_mode = WM_PEAK;
  }
  peer->size_err = 0;

  us->peer.init = ngx_http_upstream_init_observed_peer;

  return NGX_OK;
}

static void ngx_http_upstream_observed_update_nreq(ngx_http_upstream_observed_peer_data_t *fp, int delta, ngx_log_t *log) {
#if (NGX_ERROR)
  ngx_uint_t nreq;
  ngx_uint_t total_nreq;

  nreq = (fp->peers[fp->current].shared->nreq += delta);
  total_nreq = (fp->peers->shared->total_nreq += delta);

  ngx_log_debug6(NGX_LOG_DEBUG_HTTP, log, 0, "[upstream_observed] nreq for peer %ui @ %p/%p now %d, total %d, delta %d", fp->current, fp->peers, fp->peers->peer[fp->current].shared, nreq, total_nreq, delta);
#endif
}
