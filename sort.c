#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <pthread.h>
#include <unistd.h>

// Funcao de ordenacao fornecida. Não pode alterar.
void bubble_sort(int *v, int tam){
    int i, j, temp, trocou;

    for(j = 0; j < tam - 1; j++){
        trocou = 0;
        for(i = 0; i < tam - 1; i++){
            if(v[i + 1] < v[i]){
                temp = v[i];
                v[i] = v[i + 1];
                v[i + 1] = temp;
                trocou = 1;
            }
        }
        if(!trocou) break;
    }
}

// Funcao para imprimir um vetor. Não pode alterar.
void imprime_vet(unsigned int *v, int tam) {
    int i;
    for(i = 0; i < tam; i++)
        printf("%d ", v[i]);
    printf("\n");
}

// Funcao para ler os dados de um arquivo e armazenar em um vetor em memoroa. Não pode alterar.
int le_vet(char *nome_arquivo, unsigned int *v, int tam) {
    FILE *arquivo;
    
    // Abre o arquivo
    arquivo = fopen(nome_arquivo, "r");
    if (arquivo == NULL) {
        perror("Erro ao abrir o arquivo");
        return 0;
    }

    // Lê os números
    for (int i = 0; i < tam; i++)
        fscanf(arquivo, "%u", &v[i]);

    fclose(arquivo);

    return 1;
}

typedef struct {
    int *dados;
    int tamanho;
    int tarefa_id;
} Tarefa;

Tarefa *tarefas;             // Vetor de tarefas
int tarefas_pendentes;       // Total de tarefas a processar
int proxima_tarefa = 0;      // Índice da próxima tarefa a ser atribuída
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

// Função executada pelas threads
void *thread_func(void *arg) {
    int thread_id = *(int *)arg;
    while (1) {
        pthread_mutex_lock(&mutex);
        if (proxima_tarefa >= tarefas_pendentes) {
            pthread_mutex_unlock(&mutex);
            break;
        }
        int idx = proxima_tarefa++;
        pthread_mutex_unlock(&mutex);

        printf("Thread %d processando tarefa %d\n", thread_id, tarefas[idx].tarefa_id);
        bubble_sort(tarefas[idx].dados, tarefas[idx].tamanho);
    }
    return NULL;
}

int sort_paralelo(unsigned int *vetor, unsigned int tam, unsigned int ntasks, unsigned int nthreads) {
    if (nthreads < 1 || ntasks > tam) return 0;

    // Etapa 1: Divisão por faixa de valores
    int intervalo = tam / ntasks;
    int resto = tam % ntasks;

    tarefas = malloc(sizeof(Tarefa) * ntasks);
    int *temp_buckets[ntasks];   // Armazena ponteiros para os dados de cada tarefa
    int counts[ntasks];          // Quantos elementos cada tarefa possui
    int max_vals[ntasks];        // Usado para reconstruir em ordem

    for (int i = 0; i < ntasks; i++) {
        counts[i] = 0;
        temp_buckets[i] = malloc(sizeof(int) * tam); // Espaço suficiente
        max_vals[i] = (i + 1) * intervalo;
        if (i < resto) max_vals[i]++;
    }

    // Distribuição dos números por faixa
    for (unsigned int i = 0; i < tam; i++) {
        unsigned int valor = vetor[i];
        for (int j = 0; j < ntasks; j++) {
            int min = (j < resto) ? j * (intervalo + 1) : j * intervalo + resto;
            int max = (j < resto) ? min + (intervalo + 1) : min + intervalo;
            if (valor >= min && valor < max) {
                temp_buckets[j][counts[j]++] = valor;
                break;
            }
        }
    }

    // Cria as tarefas com os subvetores
    tarefas_pendentes = 0;
    for (int i = 0; i < ntasks; i++) {
        if (counts[i] > 0) {
            tarefas[tarefas_pendentes].dados = malloc(sizeof(int) * counts[i]);
            tarefas[tarefas_pendentes].tamanho = counts[i];
            tarefas[tarefas_pendentes].tarefa_id = i;
            for (int j = 0; j < counts[i]; j++) {
                tarefas[tarefas_pendentes].dados[j] = temp_buckets[i][j];
            }
            tarefas_pendentes++;
        }
        free(temp_buckets[i]); // Libera memória temporária
    }

    // Etapa 2: Execução paralela
    pthread_t threads[nthreads];
    int thread_ids[nthreads];
    for (int i = 0; i < nthreads; i++) {
        thread_ids[i] = i;
        pthread_create(&threads[i], NULL, thread_func, &thread_ids[i]);
    }
    for (int i = 0; i < nthreads; i++) {
        pthread_join(threads[i], NULL);
    }

    // Etapa 3: Concatenação dos subvetores ordenados
    int pos = 0;
    for (int i = 0; i < ntasks; i++) {
        for (int j = 0; j < tarefas_pendentes; j++) {
            if (tarefas[j].tarefa_id == i) {
                for (int k = 0; k < tarefas[j].tamanho; k++) {
                    vetor[pos++] = tarefas[j].dados[k];
                }
                free(tarefas[j].dados);
                break;
            }
        }
    }

    free(tarefas);
    return 1;
}

// Funcao principal do programa. Não pode alterar.
int main(int argc, char **argv) {
    
    // Verifica argumentos de entrada
    if (argc != 5) {
        fprintf(stderr, "Uso: %s <input> <nnumbers> <ntasks> <nthreads>\n", argv[0]);
        return 1;
    }

    // Argumentos de entrada
    unsigned int nnumbers = atoi(argv[2]);
    unsigned int ntasks = atoi(argv[3]);
    unsigned int nthreads = atoi(argv[4]);
    
    // Aloca vetor
    unsigned int *vetor = malloc(nnumbers * sizeof(unsigned int));

    // Variaveis de controle de tempo de ordenacao
    struct timeval inicio, fim;

    // Le os numeros do arquivo de entrada
    if (le_vet(argv[1], vetor, nnumbers) == 0)
        return 1;

    // Imprime vetor desordenado
    imprime_vet(vetor, nnumbers);

    // Ordena os numeros considerando ntasks e nthreads
    gettimeofday(&inicio, NULL);
    sort_paralelo(vetor, nnumbers, ntasks, nthreads);
    gettimeofday(&fim, NULL);

    // Imprime vetor ordenado
    imprime_vet(vetor, nnumbers);

    // Desaloca vetor
    free(vetor);

    // Imprime o tempo de ordenacao
    printf("Tempo: %.6f segundos\n", fim.tv_sec - inicio.tv_sec + (fim.tv_usec - inicio.tv_usec) / 1e6);
    
    return 0;
}