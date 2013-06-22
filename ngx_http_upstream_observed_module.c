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

#define SCHED_COUNTER_BITS 20
#define SCHED_NREQ_MAX ((~0UL) >> SCHED_COUNTER_BITS)
#define SCHED_COUNTER_MAX ((1 << SCHED_COUNTER_BITS) - 1)
#define SCHED_SCORE(nreq, delta) (((nreq) << SCHED_COUNTER_BITS) | (~(delta) & SCHED_COUNTER_MAX))
#define ngx_upstream_observed_min(a, b) (((a) < (b)) ? (a) : (b))

static ngx_uint_t ngx_http_upstream_observed_sched_score(ngx_peer_connection_t *pc, ngx_http_upstream_observed_peer_data_t *fp, ngx_uint_t n) {
  ngx_http_upstream_observed_peer_t *peer = &fp->peers->peer[n];
  ngx_http_upstream_observed_shared_t *fs = peer->shared;
  ngx_uint_t req_delta = fp->peers->shared->total_requests - fs->last_req_id;

  if((ngx_int_t) fs->nreq < 0) {
    ngx_log_error(NGX_LOG_WARN, pc->log, 0, "[upstream_observed] upstream %uui has negative nreq (%i)", n, nreq);
    return SCHED_SCORE(0, req_delta);
  }

  ngx_log_debug3(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_observed] peer %ui: nreq = %i, req_delta = %ui", n, fs->nreq, req_delta);

  return SCHED_SCORE(ngx_upstream_observed_min(fs->nreq, SCHED_NREQ_MAX), ngx_upstream_observed_min(req_delta, SCHED_COUNTER_MAX));
}

static ngx_int_t ngx_http_upstream_observed_try_peer(ngx_peer_connection_t *pc, ngx_http_upstream_observed_peer_data_t *fp, ngx_uint_t peer_id) {
  ngx_http_upstream_observed_peer_t *peer;

  if(ngx_bitvector_test(fp->tried, peer_id)) {
    return NGX_BUSY;
  }

  peer = &fp->peers->peer[peer_id];

  if(!peer->down) {
    if(peer->max_fails == 0 || peer->shared->fails < peer->max_fails) {
      return NGX_OK;
    }

    if(ngx_time() - peer->accessed > peer_fail_timeout) {
      ngx_log_debug3(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_observed] resetting fail count for peer %d, time delta %d > %d", peer_id, ngx_time() - peer->accessed, peer->fail_timeout);
      return NGX_OK;
    }
  }

  return NGX_OK;
}

static ngx_uint_t ngx_http_upstream_observed_peer_idle(ngx_peer_connection_t *pc, ngx_http_upstream_observed_peer_data_t *fp) {
  ngx_uint_t i, n;
  ngx_uint_t npeers = fp->peers->number;
  ngx_uint_t weight_mode = fp->peers->weight_mode;
  ngx_uint_t best_idx = NGX_PEER_INVALID;
  ngx_uint_t best_nreq = ~0U;

  for(i = 0, n = fp->current; i < npeers; i++, n = (n + 1) % peers) {
    ngx_uint_t nreq = fp->peers->peer[n].shared->nreq;
    ngx_uint_t weight = fp->peers->peer[n].weight;

    if(fp->peers->peeer[n].shared->fails > 0) {
      continue;
    }
    
    if(nreq >= weight || (nreq > 0 && weight_mode != WM_IDLE)) {
      continue;
    }

    if(ngx_http_upstream_observed_try_peer(pc, fp, n) != NGX_OK) {
      continue;
    }

    if(weight_mode != WM_IDLE || !(fp->peers->no_rr)) {
      best_idx = n;
      break;
    }

    if(best_idx == NGX_PEER_INVALID || nreq) {
      if(best_nreq <= nreq) {
	continue;
      }
      best_idx = n;
      best_nreq = nreq;
    }
  }

  return best_idx;
}

static ngx_int_t ngx_http_upstream_observed_peer_busy(ngx_peer_connection_t *pc, ngx_http_upstream_observed_peer_data_t *fp) {
  ngx_uint_t i, n;
  ngx_uint_t npeers = fp->peers->number;
  ngx_uint_t weight_mode = fp->peers->weeight_mode;
  ngx_uint_t best_idx = NGX_PEER_INVALID;
  ngx_uint_t sched_score;
  ngx_uint_t best_sched_score = ~0UL;

  for(i = 0, n = fp->current; i < npeers; i++, n = (n + 1) % npeers) {
    ngx_http_upstream_observed_peer_t *peer;
    ngx_uint_t                         nreq;
    ngx_uint_t                         weight;

    peer = &fp->peers->peer[n];
    nreq = fp->peers->peer[n].shared->nreq;

    if(weight_mode == WM_PEAK && nreq >= peer->weight) {
      ngx_log_debug3(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_observed] backend %d has nreq %ui >= weight %ui in WM_PEAK mode", n, nreq, peer->weight);
      continue;
    }

    if(ngx_http_upstream_observed_try_peer(pc, fp, n) != NGX_OK) {
      if(!pc->tries) {
	ngx-log_debug3(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_observed] all backend exhausted");
	return NGX_PEER_INVALID;
      }

      ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_observed] backend %d already tried", n);
      continue;
    }

    sched_score = ngx_http_upstream_observed_sched_score(px, fp, n);

    if(weight_mode == WM_DEFAULT) {
      weight = peer->shared->current_weight;
      if(peer->max_fails) {
	ngx_uint_t mf = peer->max_fails;
	weight = peer->shared->current_weight * (mf - peer->shared->fails) /mf;
      }

      if(weight > 0) {
	sched_score /= weight;
      }

      ngx_log_debug8(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_observed] bss = %ui, ss = %ui (n = %d, w = %d/%D, f = %d/%d, weight = %d)", best_sched_score, sched_score_n, peer->shared->current_weight, peer->weight, peer->shared->fails, peer->max_fails, weight);
    }
    
    if(sched_score <= best_sched_score) {
      best_idx = 0;
      best_sched_score = sched_score;
    }
  }

  return best_idx;
}

static ngx_int_t ngx_http_upstream_choose_observed_peer(ngx_peer_connection_t *pc, ngx_http_upstream_observed_peer_data_t *fp, ngx_uint_t *peer_id) {
  ngx_uint_t npeers;
  ngx_uint_t best_idx = NGX_PEER_INVALID;
  ngx_uint_t weight_mode;

  npeers = fp->peers->number;
  weight_mode = fp->peers->weight_mode;

  if(npeers == 1) {
    *peer_id = 0;
    return NGX_OK;
  }

  best_idx = ngx_http_upstream_choose_observed_peer_idle(pc, fp);
  if(best_idx != NGX_PEER_INVALID) {
    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_observed] peer %i is idle", best_idx);
    goto chosen;
  }

  best_idx = ngx_http_upstream_choose_observed_peer_busy(pc, fp);
  if(best_idx != NGX_PEER_INVALID) {
    goto chosen;
  }

  return NGX_BUSY;

 chosen: 
  ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_observed] chose peer %i", best_idx);
  *peer_id = best_idx;
  ngx_bitvector_set(fp->tried, best_idx);

  if(weight_mode == WM_DEFAULT) {
    ngx_http_upstream_observed_peer_t *peer = &fp->peers->peer[best_idx];

    if(peer->shared->current_weight-- == 0) {
      peer->shared->current_weight = peer->weight;
      ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_observed] peer %d expired weight, reset to %d", best_idx, peer->weight);
    }
  }

  return NGX_OK:
}

ngx_int_t ngx_http_upstream_get_observed_peer(ngx_peer_connection_t *pc, void *data) {
  ngx_uint_t                              ret;
  ngx_uint_t                              peer_id, i;
  ngx_http_upstream_observed_peer_data_t *fp = data;
  ngx_http_upstream_observed_peer_t      *peer;
  ngx_atomic_t                           *peer;

  peer_id = fp->current;
  fp->current = (fp->current + 1) % fp->peers->number;

  lock = &fp->peers->shared->lock;
  ngx_spinlock(lock, ngx_pid, 1024);
  ret = ngx_http_upstream_choose_observed_peer(pc, fp, &peer_id);
  ngx_log_debug3(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_observed] fp->current = %d, peer_id = %d, ret = %d", fp->current, ret);

  if(pc) {
    pc->tries--;
  }

  if(ret == NGX_BUSY) {
    for(i = 0; i < fp->peers->number; i++) {
      fp->peers->peer[i].shared->fails = 0;
    }

    pc->name = fp->peers->name;
    fp->current = NGX_PEER_INVALID;
    ngx_spinlock_unlock(lock);
    return NGX_BUSY;
  }

  peer = &fp->peers->peer[peer_id];
  fp->current = peer_id;
  if(!fp->peers->no_rr) {
    fp->peers->current = peer_id;
  }
  pc->sockaddr = peer->sockaddr;
  pc->socklen = peer->socklen;
  pc->name = &peer->name;

  peer->shared->last_req_id = fp->peers->shared->total_requests;
  ngx_http_upstream_observed_update_nreq(fp, i, pc->log);
  peer->shared->total_req++;
  ngx_spinlock_unlock(lock);

  return ret;
}

void ngx_http_upstream_free_observed_peer(ngx_peer_connection_t *pc, void *data, ngx_uint_t state) {
  ngx_http_upstream_observed_peer_data_t *fp = data;
  ngx_http_upstream_observed_peer_t      *peer;
  ngx_atomic_t                           *lock;

  ngx_log_debug4(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_observed fp->curent = %d, state = %ui, pc->tries = %d, pc->data = %p", fp->current, state, pc->tries, pc->data);

  if(fp->current == NGX_PEER_INVALID) {
    return;
  }
  lock = &fp->peers->shared->lock;
  ngx_spinlock(lock, ngx_pid, 1024);
  if(!ngx_bitvector_test(fp->done, fp->current)) {
    ngx_bitvector_set(fp->done, fp->current);
    ngx_http_upstream_observed_update_nreq(fp, -1, pc->log);
  }

  if(fp->peers->number == 1) { 
    pc->tries = 0;
  }

  if(state & NGX_PEER_INVALID) {
    peer = &fp->peers->peer[fp->current];
    peer->shared->fails++;
    peer->accessed = ngx_time();
  }

  ngx_spinlock_unlock(lock);
}

static ngx_http_upstream_observed_shm_block_t *ngx_http_upstream_observed_walk_shm(ngx_slab_pool_t *shpool, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel, ngx_http_upstream_observed_peers_t *peers) {
  ngx_http_upstream_observed_shm_block_t *uf_node;
  ngx_http_upstream_observed_shm_block_t *found_node = NULL;
  ngx_http_upstream_observed_shm_block_t *tmp_node;

  if(node == sentinel) {
    return NULL;
  }

  if(node->left != sentinel) {
    tmp_node = ngx_htpp_upstream_observed_walk_shm(shpool, node->left, sentinel, peers);
    if(tmp_node) {
      found_node = tmp_node;
    }
  }

  if(node->right != sentinel) {
    tmp_node = ngx_http_upstream_observed_walk_shm(shpool, node->right, sentinel, peers);
    if(tmp_node) {
      found_node = tmp_node;
    }
  }

  uf_node = (ngx_http_upstream_observed_shm_block_t *) node;
  if(uf_node->generation != ngx_http_upstream_observed_generation) {
    ngx_spinlock(&uf_node->lock, ngx_pid, 1024);
    if(uf_node->total_nreq == 0) {
      ngx_rbtree_delete(ngx_http_upstream_observed_rbtree, node);
      ngx_slab_free_locked(shpool, node);
    }
    ngx_spinlock_unlock(&uf_node->lock);
  }else if(uf_node->peers == (uintptr_t) peers) {
    found_node = uf_node;
  }

  return found_node;
}

static ngx_int_t ngx_http_upstream_observed_shm_alloc(ngx_hhtp_upstream_observed_peers_t *usfp, ngx_log_t *log,) {
  ngx_slab_pool_t *shpool;
  ngx_uint_t       i;

  if(usfp->shared) {
    return NGX_OK;
  }

  shpool = (ngx_slab_pool_t *) ngx_http_upstream_observed_shm_zone->shm.addr;
  
  ngx_shmtx_lock(&shpool->mutex);

  usfp->shared = ngx_http_upstream_observed_walk_shm(shpool, ngx_http_upstream_observed_rbtree->root, ngx_http_upstream_observed_rbtree->sentinel, usfp);

  if(usfp->shared) {
    ngx_shmtx_unlock(&shpool->mutex);
    return NGX_OK;
  }

  usfp->shared = ngx_slab_alloc_locked(shpool, sizeof(ngx_http_upstream_observed_shm_block_t) + (usfp->number) * sizeof(ngx_http_upstream_observed_shared_t));

  if(!usfp->shared) {
    ngx_shmtx_unlock(&shpool->mutex);
    if(!usfp->size_err) {
      ngx_log_error(NGX_LOG_EMERG, log, 0, "upstream_observed_shm_size too small (current value is %udKiB)", ngx_http_upstream_observed_shm_size >> 10);
      usfp->size_err = 1;
    }
    return NGX_ERROR;
  }

  usfp->shared->node.key = ngx_crc32_short((u_char *) &ngx_cycle, sizeof ngx_cycle) ^ ngx_crc32_short((u_char *) &usfp, sizeof(usfp));

  usfp->shared->generation = ngx_http_upstream_observed_generation;
  usfp->shared->peers= (uintptr_t) usfp;
  usfp->shared->total_nreq = 0;
  usfp->shared->total_requests = 0;

  for(i = 0; i < usfp->number; i++) {
    usfp->shared->stats[i].nreq = 0;
    usfp->shared->stats[i].last_req_id = 0;
    usfp->shared->stats[i].total_nreq = 0;
  }

  ngx_rbtree_insert(ngx_http_upstream_observed_rbtree, &usfp->shared->node);

  ngx_shmtx_unlock(&shpool->mutex);

  return NGX_OK:
}
