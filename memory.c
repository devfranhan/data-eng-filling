#include <stdio.h>

int main(void){
    // printf("In C's memory!!!\n");

    int n = 50;
    int *p = &n;

    printf("%p\n", p);      // imprime o endereço onde n está guardado
    printf("%i\n", *p);     // imprime o valor guardado nesse endereço (50)
    
}
