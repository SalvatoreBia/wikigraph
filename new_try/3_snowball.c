#include <glib.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>

#define SEEDS 4
#define K     2

/*
su arch tocca fa così 

gcc 3_snowball.c -o snowball $(pkg-config --cflags --libs glib-2.0)

./snowball_app finalmap.csv

*/

// +------------------+
// | GLOBAL VARIABLES |
// +------------------+
GHashTable *map;

struct node
{

    int idx;
    GSList *neighbors;

};

// +------------------+
// | HELPER FUNCTIONS |
// +------------------+
struct node* find_or_create_node(int id)
{
    gpointer key = GINT_TO_POINTER(id);
    gpointer value = g_hash_table_lookup(map, key);

    struct node* n;

    if (value == NULL)
    {
        n = g_new(struct node, 1);
        n->idx = id;
        n->neighbors = NULL;

        g_hash_table_insert(map, key, n);
    }
    else
    {
        n = (struct node*) value;
    }

    return n;
}

void node_destroy_func(gpointer key, gpointer value, gpointer user_data)
{
    struct node* n = (struct node*) value;
    g_slist_free(n->neighbors);
    g_free(n);
}


// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

/*
 * Esegue il campionamento a palla di neve (snowball sampling) partendo da un nodo 'seed_id'.
 * L'algoritmo si svolge in due fasi:
 * 
 * 1. FASE A (Ricerca Nodi): Esegue una ricerca in ampiezza (BFS) per K 'onde'.
 * Partendo dal seme, trova tutti i vicini (Onda 1), poi i vicini dei vicini (Onda 2), ecc.
 * Tutti i nodi unici trovati vengono memorizzati in un set 'sampled_nodes'.
 * 
 * 2. FASE B (Estrazione Archi): Itera su tutti i nodi trovati (il set 'sampled_nodes').
 * Per ogni nodo, controlla i suoi vicini *originali*. Se un vicino è *anch'esso*
 * presente nel set 'sampled_nodes', l'arco tra loro viene salvato nel file di output.
 * Questo crea il "sottografo indotto" del campione.
 */
void snowball_sample(int seed_id, int ith)
{
    fprintf(stdout, "\tStarting sample %d (Seed: %d, K: %d)\n", ith, seed_id, K);

    // === FASE A: TROVARE I NODI (BFS) ===
    
    GHashTable* sampled_nodes = g_hash_table_new(g_direct_hash, g_direct_equal);

    GSList* frontier = NULL;
    GSList* next_frontier = NULL;

    gpointer seed_key = GINT_TO_POINTER(seed_id);
    g_hash_table_insert(sampled_nodes, seed_key, (gpointer)1);
    frontier = g_slist_prepend(frontier, seed_key);

    for (int i = 0; i < K; i++)
    {
        next_frontier = NULL;
        GSList* f_iter = NULL; 

        for (f_iter = frontier; f_iter != NULL; f_iter = f_iter->next)
        {
            gpointer current_key = f_iter->data;
            struct node* current_node = g_hash_table_lookup(map, current_key);

            if (current_node == NULL) continue;

            GSList* n_iter = NULL; 
            
            for (n_iter = current_node->neighbors; n_iter != NULL; n_iter = n_iter->next)
            {
                gpointer neighbor_key = n_iter->data;
                
                if (g_hash_table_lookup(sampled_nodes, neighbor_key) == NULL)
                {
                    g_hash_table_insert(sampled_nodes, neighbor_key, (gpointer)1);
                    next_frontier = g_slist_prepend(next_frontier, neighbor_key);
                }
            }
        }

        g_slist_free(frontier);
        frontier = next_frontier;

        if (frontier == NULL) {
            break;
        }
    }

    g_slist_free(frontier); 
    
    fprintf(stdout, "\t\t-> Node phase complete. Nodes found: %u\n", g_hash_table_size(sampled_nodes));


    // === FASE B: ESTRARRE GLI ARCHI (SOTTOGRAFO INDOTTO) ===

    char filename[100];
    sprintf(filename, "sample_%d.csv", ith); 

    FILE* output_file = fopen(filename, "w");
    if (output_file == NULL)
    {
        fprintf(stderr, "\t\t***ERROR: Could not create file %s\n", filename);
        g_hash_table_destroy(sampled_nodes);
        return;
    }

    fprintf(output_file, "src_page,dest_page\n");

    GHashTableIter iter;
    gpointer node_u_key, dummy_value;
    g_hash_table_iter_init(&iter, sampled_nodes);

    while (g_hash_table_iter_next(&iter, &node_u_key, &dummy_value))
    {
        int node_u_id = GPOINTER_TO_INT(node_u_key);
        struct node* node_u = g_hash_table_lookup(map, node_u_key);
        
        if (node_u == NULL) continue;

        GSList* n_iter = NULL;
        for (n_iter = node_u->neighbors; n_iter != NULL; n_iter = n_iter->next)
        {
            gpointer node_v_key = n_iter->data;
            int node_v_id = GPOINTER_TO_INT(node_v_key);

            if (node_u_id < node_v_id)
            {
                if (g_hash_table_lookup(sampled_nodes, node_v_key) != NULL)
                {
                    fprintf(output_file, "%d,%d\n", node_u_id, node_v_id);
                }
            }
        }
    }

    fclose(output_file);
    g_hash_table_destroy(sampled_nodes); 

    fprintf(stdout, "\t\t-> Edge phase complete. File saved: %s\n", filename);
}


// add or update src and dest entry in the hash table
void add_nodes(int src, int dest)
{
    struct node* src_node  = find_or_create_node(src);
    struct node* dest_node = find_or_create_node(dest);

    src_node->neighbors  = g_slist_prepend(src_node->neighbors, GINT_TO_POINTER(dest));
    dest_node->neighbors = g_slist_prepend(dest_node->neighbors, GINT_TO_POINTER(src));
}


int main(int argc, char **argv)
{
    if (argc != 2)
    {
        fprintf(stderr, "Usage: %s <input_file>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    FILE *input = fopen(argv[1], "r");
    if (input == NULL)
    {
        fprintf(stderr, "***ERROR: Couldn't open input file. Exiting...\n");
        exit(EXIT_FAILURE);
    }

    map = g_hash_table_new(g_direct_hash, g_direct_equal);


    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    while ((read = getline(&line, &len, input)) != -1)
    {
        int src, dest;

        char *ptr = line;
        char *endptr1;
        char *endptr2;

        char *comma = strchr(ptr, ',');
        long src_lid = strtol(ptr, &endptr1, 10);

        if (endptr1 != comma)
        {
            fprintf(stderr, "***Error: ID parsing mismatch.\n");
            return 1;
        }

        ptr = comma + 1;
        long dest_lid = strtol(ptr, &endptr2, 10);
        if (ptr == endptr2 || (*endptr2 != '\n' && *endptr2 != '\0'))
        {
            fprintf(stderr, "***Error: Failed to parse target ID.\n");
            return 1;
        }

        src =  (int)src_lid;
        dest = (int)dest_lid;

        add_nodes(src, dest);
    }

    free(line);
    fclose(input);

    fprintf(stdout, "Graph loaded. Number of nodes: %u\n", g_hash_table_size(map));

    int seeds[SEEDS];
    GList *keys = g_hash_table_get_keys(map);
    guint nnodes = g_list_length(keys);

    srand(time(NULL));

    fprintf(stdout, "Selecting %d seeds...\n", SEEDS);
    for (int i = 0; i < SEEDS; ++i)
    {
        guint random_idx = rand() % nnodes;
        gpointer data = g_list_nth_data(keys, random_idx);

        int seed_id = GPOINTER_TO_INT(data);

        seeds[i] = seed_id;

        fprintf(stdout, "\tSeed %d: %d\n", i+1, seed_id);
    }

    g_list_free(keys);
    keys = NULL;

    fprintf(stdout, "Executing snowball sampling...\n");
    for (int i = 0; i < SEEDS; ++i)
    {
        snowball_sample(seeds[i], i);
    }
    
    fprintf(stdout, "All samples completed.\n");
    
    fprintf(stdout, "Cleaning up graph memory...\n");
    g_hash_table_foreach(map, (GHFunc)node_destroy_func, NULL);
    g_hash_table_destroy(map);
    
    fprintf(stdout, "Cleanup complete. Exiting.\n");
    return 0;
}