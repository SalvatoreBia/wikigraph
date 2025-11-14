#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <sys/types.h>
//#include "/home/salvatroie/MISC/include/misc/vector.h"
//#include "/home/salvatroie/MISC/include/misc/htable.h"


typedef struct
{

    int degree;
    misc_vector neighbors;

} node;


typedef struct
{
    int u;
    int v;

    double w;
} edge;


misc_htable map;
misc_vector edges;



// Comparatore per qsort degli interi
int compare_ints(const void *a, const void *b)
{
    int ia = *(const int*)a;
    int ib = *(const int*)b;
    return (ia > ib) - (ia < ib);
}

int add_or_update(int src, int dest)
{
    if (!misc_htable_contains(map, &src))
    {
        node new_node;
        new_node.degree = 0;
        new_node.neighbors = misc_vector_create(sizeof(int));
        if (new_node.neighbors == NULL) return 0;

        misc_htable_put(map, &src, &new_node);
    }

    node *n = (node*)misc_htable_get(map, &src);
    if (n == NULL) return 0;

    if (!misc_vector_pushback(n->neighbors, &dest)) return 0;
    n->degree++;

    return 1;
}

// Funzione per ordinare i neighbors di un nodo
void sort_neighbors(const void *key, void *value, void *user_data)
{
    node *n = (node*)value;
    if (n->degree > 0)
    {
        // Ottieni puntatore diretto all'array interno del vector
        int *neighbors_array = (int*)misc_vector_get(n->neighbors, 0);
        qsort(neighbors_array, n->degree, sizeof(int), compare_ints);
    }
}

// Funzione per liberare la memoria dei neighbors di ogni nodo
void cleanup_node(const void *key, void *value, void *user_data)
{
    node *n = (node*)value;
    if (n->neighbors != NULL)
    {
        misc_vector_destroy(n->neighbors);
        n->neighbors = NULL;
    }
}


/*
    void process_entry(const void *key, void *value, void *user_data)
{
    // 2. Fai il cast della chiave e del valore ai tipi corretti
    int actual_key = *(const int*)key;      // La chiave è un int
    int actual_value = *(int*)value;         // Il valore è un int
    
    // 3. Ora puoi usare la chiave e il valore come vuoi
    printf("Chiave: %d, Valore: %d\n", actual_key, actual_value);
}
}*/
void add_weight(const void *key, void *value, void *user_data)
{
    int u = *(const int*)key;
    node *n_u = (node*)value;

    // Itera sui vicini di u
    for (int i = 0; i < n_u->degree; ++i)
    {
        int v = *(int*)misc_vector_get(n_u->neighbors, i);

        // Evita archi duplicati: processa solo se u < v
        if (u < v)
        {
            // Recupera il nodo v
            node *n_v = (node*)misc_htable_get(map, &v);
            if (n_v == NULL) continue; // Sicurezza

            // Calcola Adamic-Adar score usando merge two-pointer
            // (i vettori sono ordinati!)
            double aa_score = 0.0;
            
            int idx_u = 0;
            int idx_v = 0;
            
            // Merge two-pointer algorithm per trovare vicini comuni
            while (idx_u < n_u->degree && idx_v < n_v->degree)
            {
                int neighbor_u = *(int*)misc_vector_get(n_u->neighbors, idx_u);
                int neighbor_v = *(int*)misc_vector_get(n_v->neighbors, idx_v);
                
                if (neighbor_u == neighbor_v)
                {
                    // Trovato vicino comune 'z'
                    int z = neighbor_u;
                    
                    // Recupera il grado di z per Adamic-Adar
                    node *n_z = (node*)misc_htable_get(map, &z);
                    
                    if (n_z != NULL && n_z->degree > 1)
                    {
                        // Formula Adamic-Adar: 1 / log(degree(z))
                        aa_score += 1.0 / log((double)n_z->degree);
                    }
                    
                    idx_u++;
                    idx_v++;
                }
                else if (neighbor_u < neighbor_v)
                {
                    idx_u++;
                }
                else
                {
                    idx_v++;
                }
            }

            // Calcola il peso: 1 / (1 + AA_score)
            // Più alto è lo score, più basso è il peso (distanza)
            double weight = 1.0 / (1.0 + aa_score);

            // Salva l'arco pesato
            edge e = { u, v, weight };
            if (!misc_vector_pushback(edges, &e))
            {
                fprintf(stderr, "***Warning: failed to add edge (%d, %d)\n", u, v);
            }
        }
    }
}


int main(int argc, char **argv)
{
    if (argc != 3)
    {
        printf("***Error: command usage -> %s finalmap.csv <output_file>\n", argv[0]);
        return 1;
    }

    char *output = argv[2];

    FILE *fp = fopen(argv[1], "r");
    if (fp == NULL)
    {
        printf("***Error: couldn't open %s file. Exiting...\n", argv[0]);
        return 1;
    }

    map = misc_htable_create_int(sizeof(node));
    edges = misc_vector_create(sizeof(edge));
    if (map == NULL || edges == NULL)
    {
        printf("***Error: misc_htable/misc_vector handle allocation failed. Exiting...\n");
        return 1;
    }

    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    while ((read = getline(&line, &len, fp)) != -1)
    {
        int src, dest;

        char *ptr = line;
        char *endptr1;
        char *endptr2;

        char *comma = strchr(ptr, ',');
        long src_lid = strtol(ptr, &endptr1, 10);

        if (endptr1 != comma)
        {
            printf("***Error: ids not matching.\n");
            return 1;
        }

        ptr = comma + 1;
        long dest_lid = strtol(ptr, &endptr2, 10);
        if (ptr == endptr2 || (*endptr2 != '\n' && *endptr2 != '\0'))
        {
            printf("***Error: failed to parse target id. Exiting...\n");
            return 1;
        }

        src =  (int)src_lid;
        dest = (int)dest_lid;

        if (!add_or_update(src, dest))
        {
            printf("***Error: failed to add/update (src, dest). Exiting...\n");
            return 1;
        }

        if (!add_or_update(dest, src))
        {
            printf("***Error: failed to add/update (dest, src). Exiting...\n");
            return 1;
        }
    }


    printf("Graph loaded. Nodes: %zu\n", misc_htable_size(map));
    
    // Ordina i neighbors di ogni nodo per velocizzare la ricerca dei vicini comuni
    printf("Sorting neighbors...\n");
    misc_htable_foreach(map, sort_neighbors, NULL);
    printf("Neighbors sorted.\n");
    
    // Calcola i pesi Adamic-Adar
    printf("Computing Adamic-Adar weights...\n");
    misc_htable_foreach(map, add_weight, NULL);
    printf("Weighted edges computed: %zu\n", misc_vector_length(edges));

    // Scrivi gli archi pesati nel file di output
    FILE *fp_out = fopen(output, "w");
    if (fp_out == NULL)
    {
        printf("***Error: couldn't open output file %s. Exiting...\n", output);
        return 1;
    }

    fprintf(fp_out, "source,target,weight\n");
    for (size_t i = 0; i < misc_vector_length(edges); i++)
    {
        edge *e = (edge*)misc_vector_get(edges, i);
        fprintf(fp_out, "%d,%d,%.10f\n", e->u, e->v, e->w);
    }

    fclose(fp_out);
    printf("Results written to %s\n", output);
    

    fclose(fp);
    
    // Cleanup: distruggi i vector dei neighbors prima di distruggere la hash table
    printf("Cleaning up memory...\n");
    misc_htable_foreach(map, cleanup_node, NULL);
    
    misc_htable_destroy(map);
    misc_vector_destroy(edges);
    return 0;
}
