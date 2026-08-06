/**
 * @file task_group.c
 * @brief Task group implementation.
 *
 * A task group tracks submitted tasks via a linked list of user_data
 * pointers.  loom_task_group_cancel() walks this list and calls
 * loom_pool_cancel() for each entry.
 */
#define _POSIX_C_SOURCE 200809L
#include "loomworks/task_group.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

/* ================================================================
 *  Internal node — tracks one submitted task's user_data pointer.
 * ================================================================ */
typedef struct task_group_node {
    void                   *user_data; /**< Pointer passed to loom_pool_submit(). */
    struct task_group_node *next;      /**< Next node in the tracked list. */
} task_group_node_t;

/* ================================================================
 *  loom_task_group — internal structure.
 * ================================================================ */
struct loom_task_group {
    loom_thread_pool_t *pool;       /**< Backing thread pool. */
    pthread_mutex_t     lock;       /**< Guards node list. */
    task_group_node_t  *head;       /**< Head of tracked node list. */
    task_group_node_t  *tail;       /**< Tail of tracked node list. */
    uint32_t            node_count; /**< Number of tracked submissions. */
    bool                destroyed;  /**< true once destroy() has been called. */
};

/* ================================================================
 *  task_group_node_create — allocate and initialise a node.
 * ================================================================ */
static task_group_node_t *task_group_node_create(void *user_data)
{
    task_group_node_t *node = (task_group_node_t *)malloc(sizeof(*node));
    if (!node) {
        return NULL;
    }
    node->user_data = user_data;
    node->next      = NULL;
    return node;
}

/* ================================================================
 *  task_group_node_destroy — free a single node.
 * ================================================================ */
static void task_group_node_destroy(task_group_node_t *node)
{
    free(node);
}

/* ================================================================
 *  loom_task_group_create
 * ================================================================ */
loom_result_t loom_task_group_create(loom_thread_pool_t *pool, loom_task_group_t **group)
{
    if (!pool || !group) {
        return LOOMWORKS_ERR_INVALID;
    }
    loom_task_group_t *g = (loom_task_group_t *)calloc(1, sizeof(*g));
    if (!g) {
        return LOOMWORKS_ERR_ALLOC;
    }
    g->pool       = pool;
    g->head       = NULL;
    g->tail       = NULL;
    g->node_count = 0;
    g->destroyed  = false;
    if (pthread_mutex_init(&g->lock, NULL) != 0) {
        free(g);
        return LOOMWORKS_ERR_ALLOC;
    }
    *group = g;
    return LOOMWORKS_OK;
}

/* ================================================================
 *  loom_task_group_destroy
 * ================================================================ */
void loom_task_group_destroy(loom_task_group_t **group)
{
    if (!group || !*group) {
        return;
    }
    loom_task_group_t *g = *group;
    pthread_mutex_lock(&g->lock);
    g->destroyed = true;
    /* Cancel any remaining tracked tasks. */
    task_group_node_t *cur = g->head;
    while (cur) {
        task_group_node_t *next = cur->next;
        loom_pool_cancel(g->pool, cur->user_data);
        task_group_node_destroy(cur);
        cur = next;
    }
    g->head       = NULL;
    g->tail       = NULL;
    g->node_count = 0;
    pthread_mutex_unlock(&g->lock);
    pthread_mutex_destroy(&g->lock);
    free(g);
    *group = NULL;
}

/* ================================================================
 *  loom_task_group_submit
 * ================================================================ */
loom_result_t loom_task_group_submit(loom_task_group_t *group, loom_task_fn fn, void *data)
{
    if (!group || !fn) {
        return LOOMWORKS_ERR_INVALID;
    }
    pthread_mutex_lock(&group->lock);
    if (group->destroyed) {
        pthread_mutex_unlock(&group->lock);
        return LOOMWORKS_ERR_INVALID;
    }
    loom_result_t rc = loom_pool_submit(group->pool, fn, data);
    if (rc == LOOMWORKS_OK && data != NULL) {
        /* Track the submission so we can cancel it later. */
        task_group_node_t *node = task_group_node_create(data);
        if (node) {
            if (group->tail) {
                group->tail->next = node;
            } else {
                group->head = node;
            }
            group->tail = node;
            group->node_count++;
        }
    }
    pthread_mutex_unlock(&group->lock);
    return rc;
}

/* ================================================================
 *  loom_task_group_submit_future
 * ================================================================ */
loom_result_t loom_task_group_submit_future(loom_task_group_t  *group,
                                            loom_task_fn_result fn,
                                            void               *data,
                                            loom_future_t     **future)
{
    if (!group || !fn) {
        return LOOMWORKS_ERR_INVALID;
    }
    pthread_mutex_lock(&group->lock);
    if (group->destroyed) {
        pthread_mutex_unlock(&group->lock);
        return LOOMWORKS_ERR_INVALID;
    }
    loom_result_t rc = loom_pool_submit_future(group->pool, fn, data, future);
    if (rc == LOOMWORKS_OK && data != NULL && future) {
        task_group_node_t *node = task_group_node_create(data);
        if (node) {
            if (group->tail) {
                group->tail->next = node;
            } else {
                group->head = node;
            }
            group->tail = node;
            group->node_count++;
        }
    }
    pthread_mutex_unlock(&group->lock);
    return rc;
}

/* ================================================================
 *  loom_task_group_cancel
 * ================================================================ */
void loom_task_group_cancel(loom_task_group_t *group)
{
    if (!group) {
        return;
    }
    pthread_mutex_lock(&group->lock);
    task_group_node_t *cur = group->head;
    while (cur) {
        task_group_node_t *next = cur->next;
        loom_pool_cancel(group->pool, cur->user_data);
        task_group_node_destroy(cur);
        cur = next;
    }
    group->head       = NULL;
    group->tail       = NULL;
    group->node_count = 0;
    pthread_mutex_unlock(&group->lock);
}

/* ================================================================
 *  loom_task_group_wait
 * ================================================================ */
void loom_task_group_wait(loom_task_group_t *group)
{
    if (!group) {
        return;
    }
    loom_pool_shutdown(group->pool);
}

/* ================================================================
 *  loom_task_group_pending_count
 * ================================================================ */
uint32_t loom_task_group_pending_count(const loom_task_group_t *group)
{
    if (!group) {
        return 0;
    }
    pthread_mutex_lock((pthread_mutex_t *)&group->lock);
    uint32_t n = group->node_count;
    pthread_mutex_unlock((pthread_mutex_t *)&group->lock);
    return n;
}
