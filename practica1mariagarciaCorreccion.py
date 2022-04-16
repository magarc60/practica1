from multiprocessing import Process
from multiprocessing import BoundedSemaphore, Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Value, Array
from time import sleep
from random import random, randint

N = 5
K = 10
NPROD = 3
NCONS = 1


def delay(factor=3):
    sleep(random()/factor)

def add_data(storage, index, data, mutex):
    mutex.acquire()
    try:
        storage[index.value] = data
        delay(6)
        index.value = index.value + 1
    finally:
        mutex.release()

def get_data(storage, index, mutex):
    mutex.acquire()
    try:
        data = storage[0]
        index.value = index.value - 1
        delay(6)
        for i in range(index.value):
            storage[i] = storage[i + 1]
        storage[index.value] = -1
    finally:
        mutex.release()
    return data

def producer(storage_lst, index_lst, empty_lst, non_empty_lst, mutex_lst):
    last = 0
    for v in range(N):
        print(f"producer {current_process().name} produciendo")
        delay(6)
        empty_lst.acquire()
        last = last + randint(0, 15)
        add_data(storage_lst, index_lst, last, mutex_lst)
        non_empty_lst.release()
        print(f"producer {current_process().name} almacenado {v}")
    empty_lst.acquire()
    add_data(storage_lst, index_lst, -1, mutex_lst)
    non_empty_lst.release()
    
    
############# para un numero aleatorio de productores ###############
def productoresRest(storage_lst, index_lst):
    result = []
    for last in range(NPROD):
        result.append(storage_lst[last][0]!=-1)
    return result

def valoresyposiciones(result,storage_lst):
    values=[]
    posiciones=[] #devuelve dos listas, una con los valores y otra con los indices
    for j in range(NPROD):
        if result[j]:
            values.append(storage_lst[j][0])
            posiciones.append(j)
    return values , posiciones #para saber los valores y sus correspondientes indices

def minimo_lst(values, posiciones): 
    min_value=min(values)
    min_index=values.index(min_value)
    minimo_index=posiciones[min_index]
    return min_value,minimo_index

def merge(storage_lst, index_lst,mutex_lst): #saco los valores menores del resultado de la funcion anterior, para poder ordenarlo luego
    result= productoresRest(storage_lst,index_lst)
    values= valoresyposiciones(result,storage_lst)[0]
    posiciones=valoresyposiciones(result,storage_lst)[1]
    if values!=[]:  #si sigue habiendo  valores..
       minimo_value = minimo_lst(values,posiciones)[0]
       minimo_index=minimo_lst(values,posiciones)[1]
       get_data(storage_lst[minimo_index], index_lst[minimo_index], mutex_lst[minimo_index])
    return minimo_value , minimo_index

#Para ver si hay productores: 
def hayProducers(storage_lst,index_lst):
    t = productoresRest(storage_lst,index_lst)
    for last in range(NPROD): 
        if t[last]:
           return True
    return False

##################################################


def consumer(storage_lst, index_lst, empty_lst, non_empty_lst, mutex_lst, sorted_data_lst):
    for last in range(NPROD):
        non_empty_lst[last].acquire()
    i=0   
    while hayProducers(storage_lst,index_lst): #mientras quede algun productor
        minimo_value, minimo_index = merge(storage_lst, index_lst, mutex_lst)
        print(f"consumer {current_process().name} deslmacenando")
        sorted_data_lst[i] = minimo_value #añadimos el minimo valor a la lista final
        empty_lst[minimo_index].release()
        non_empty_lst[minimo_index].acquire()
        print(f"consumer {current_process().name} consumiendo {minimo_value}")
        i += 1

def main():
    sorted_data_lst = Array('i', N*NPROD)
    storage_lst = [Array('i', K) for i in range(NPROD)]
    index_lst = [Value('i', 0) for i in range(NPROD)]
    non_empty_lst = [Semaphore(0) for i in range(NPROD)]
    empty_lst = [BoundedSemaphore(K) for i in range(NPROD)]
    mutex_lst = [Lock() for i in range(NPROD)]

    prodlst = [Process(target=producer,
                       name=f'prod_{i}',
                       args=(storage_lst[i], index_lst[i], empty_lst[i], non_empty_lst[i], mutex_lst[i]))for i in range(NPROD)]

    conslst = [Process(target=consumer,
                       name=f"cons_{i}",
                       args=(storage_lst, index_lst, empty_lst, non_empty_lst, mutex_lst, sorted_data_lst)) for i in range(NCONS)]

    for p in prodlst + conslst:
        p.start()

    for p in prodlst + conslst:
        p.join()
    
    print(sorted_data_lst[:])

if __name__ == '__main__':
    main()

