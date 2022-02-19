#include<stdio.h>
#include<mpi.h>
#define NUM_ROWS_X 5
#define NUM_COLS_X 32
#define NUM_ROWS_Y 32
#define NUM_COLS_Y 5
#define MASTER_TO_SLAVE_TAG 1 //tag for messages sent from master to slaves
#define SLAVE_TO_MASTER_TAG 4 //tag for messages sent from slaves to master
void generateAB();
void displayArray();

int rank;
int size; //number of processes
int i, j, k; //helper variables
int mat_a[NUM_ROWS_X][NUM_COLS_X]; 
int mat_b[NUM_ROWS_Y][NUM_COLS_Y]; 
int mat_result[NUM_ROWS_X][NUM_COLS_Y]; 
int start_time; 
int end_time; 
int lower_limit; //lower limit of the number of rows of [A] allocated to a slave
int upper_limit; //upper limit of the number of rows of [A] allocated to a slave
int portion; //portion of the number of rows of [A] allocated to a slave
MPI_Status status; // store status of a MPI_Recv
MPI_Request request; //capture request of a MPI_Isend

int main(int argc, char* argv[])
{

    MPI_Init(&argc, &argv); //initialize MPI operations
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); //get the rank
    MPI_Comm_size(MPI_COMM_WORLD, &size); //get number of processes

    if (rank == 0) {    // master initializes work
        generateAB();
        start_time = MPI_Wtime();
        portion = (NUM_ROWS_X / (size - 1)); // calculate portion (excluding master)
        for (i = 1; i < size; i++) {//for each slave other than the master
            lower_limit = (i - 1) * portion;
            if (((i + 1) == size) && ((NUM_ROWS_X % (size - 1)) != 0)) {    //if rows of [A] cannot be equally divided among slaves
                upper_limit = NUM_ROWS_X; // assign all the remaining rows to the last slave
            }
            else {
                upper_limit = lower_limit + portion; //rows of [A] are equally divisable among slaves
            }
            MPI_Isend(&lower_limit, 1, MPI_INT, i, MASTER_TO_SLAVE_TAG, MPI_COMM_WORLD, &request);  //sends the lower limit first without blocking, to the current slave
            MPI_Isend(&upper_limit, 1, MPI_INT, i, MASTER_TO_SLAVE_TAG + 1, MPI_COMM_WORLD, &request);  //sends the upper limit without blocking, to the current slave
            // sends the allocated row portion of [A] without blocking, to the current slave
            MPI_Isend(&mat_a[lower_limit][0], (upper_limit - lower_limit) * NUM_COLS_X, MPI_INT, i, MASTER_TO_SLAVE_TAG + 2, MPI_COMM_WORLD, &request);
        }
    }
	MPI_Bcast(&mat_b, NUM_ROWS_Y* NUM_COLS_Y, MPI_INT, 0, MPI_COMM_WORLD);   // broadcasts mat_B to all the slaves

    if (rank > 0) { // work done by slaves
        MPI_Recv(&lower_limit, 1, MPI_INT, 0, MASTER_TO_SLAVE_TAG, MPI_COMM_WORLD, &status);    // receives lower limit from the master
        MPI_Recv(&upper_limit, 1, MPI_INT, 0, MASTER_TO_SLAVE_TAG + 1, MPI_COMM_WORLD, &status);    //receives upper limit from the master
        // receives row portion of [A] to be processed from the master
        MPI_Recv(&mat_a[lower_limit][0], (upper_limit - lower_limit) * NUM_COLS_X, MPI_INT, 0, MASTER_TO_SLAVE_TAG + 2, MPI_COMM_WORLD, &status);
        
        for (i = lower_limit; i < upper_limit; i++) {
            for (j = 0; j < NUM_COLS_Y; j++) {
                for (k = 0; k < NUM_ROWS_Y; k++) {
                    mat_result[i][j] += (mat_a[i][k] * mat_b[k][j]);
                }
            }
        }
        MPI_Isend(&lower_limit, 1, MPI_INT, 0, SLAVE_TO_MASTER_TAG, MPI_COMM_WORLD, &request);  // sends back the lower limit first without blocking, to the master
        MPI_Isend(&upper_limit, 1, MPI_INT, 0, SLAVE_TO_MASTER_TAG + 1, MPI_COMM_WORLD, &request);  // sends the upper limit next without blocking, to the master
        // sends the processed portion of data without blocking, to the master
        MPI_Isend(&mat_result[lower_limit][0], (upper_limit - lower_limit) * NUM_COLS_Y, MPI_INT, 0, SLAVE_TO_MASTER_TAG + 2, MPI_COMM_WORLD, &request);
    }

    if (rank == 0) {    // master gathers processed work
        for (i = 1; i < size; i++) {// untill all slaves have handed back the processed data
            MPI_Recv(&lower_limit, 1, MPI_INT, i, SLAVE_TO_MASTER_TAG, MPI_COMM_WORLD, &status);
            MPI_Recv(&upper_limit, 1, MPI_INT, i, SLAVE_TO_MASTER_TAG + 1, MPI_COMM_WORLD, &status);
            MPI_Recv(&mat_result[lower_limit][0], (upper_limit - lower_limit) * NUM_COLS_Y, MPI_INT, i, SLAVE_TO_MASTER_TAG + 2, MPI_COMM_WORLD, &status);
        }
        end_time = MPI_Wtime();
        printf("\nTime Taken: %f\n\n", end_time - start_time);
        displayArray();
    }
    MPI_Finalize(); //finalize MPI operations
    return 0;
}

void generateAB() {
    for (i = 0; i < NUM_ROWS_X; i++) {
        for (j = 0; j < NUM_COLS_X; j++) {
            mat_a[i][j] = i + j;
        }
    }
    for (i = 0; i < NUM_ROWS_Y; i++) {
        for (j = 0; j < NUM_COLS_Y; j++) {
            mat_b[i][j] = i + j;
        }
    }
}

void displayArray() {
    printf("\nMatrix X:\n");
    for (i = 0; i < NUM_ROWS_X; i++) {
        printf("\n");
        for (j = 0; j < NUM_COLS_X; j++)
            printf("%4d  ", mat_a[i][j]);
    }
    printf("\n\n");
    printf("\nMatrix Y:\n");
    for (i = 0; i < NUM_ROWS_Y; i++) {
        printf("\n");
        for (j = 0; j < NUM_COLS_Y; j++)
            printf("%4d  ", mat_b[i][j]);
    }
    printf("\n\n");        
    printf("\nResult Matrix:\n");
    for (i = 0; i < NUM_ROWS_X; i++) {
        printf("\n");
        for (j = 0; j < NUM_COLS_Y; j++)
            printf("%4d  ", mat_result[i][j]);
    }
}