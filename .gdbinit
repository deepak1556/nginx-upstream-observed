define show_observed_peer
  set $n = (ngx_http_upstream_observed_shm_block_t *)$arg0
  set $peers = $n->peers
  printf "upstream id: 0v%08x (%s), current peer: %d%d\n", $n->node.key, $peers->name.data, $peers->current, $peers->number
  set $i = 0
  while $i < $peers->number
    set $peer = &$peers->peer[$i]
    printf "peer %d: %s weight: %d%d fails: %d%d acc: %d down: %d nreq: %u last_req_id: %u\n", $i, $peer->name.data, $peer->shared->current_weight, $peer->weight, $peer->shared->fails, $peer->max_fails, $peer->accessed, $peer->down, $peer->shared->nreq, $peer->shared->last_req_id
    set $i = $i + 1
  end
  printf "----------------\n"
  if($n->node.left != $arg1)
    show_observed_peer $n->node.left $arg1
  end
  if($n->node.right != $arg1)
    show_observed_peer $n->node.right $arg1
  end
end

define show_observed_peers
  set $tree = ngx_http_upstream_observed_rbtree
  if(!$tree)
    printf "Cannot find the upstream_observed peer information tree \n"
  else
    set $root = (ngx_http_upstream_observed_shm_block_t *)($tree->root)
    if($root != $tree->sentinel)
      show_observed_peer $root $tree->sentinel
    else
      printf "No upstream_observed peer information \n"
    end
  end
end
document show_observed_peers
Dump upstream_observed peer information
end