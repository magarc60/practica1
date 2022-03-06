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
        delay()
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


def productoresRest(storage_lst):
    result = []
    if storage_lst[0][0] != -1:
        result.append(True)
    if storage_lst[0][0] == -1:
        result.append(False)
    if storage_lst[1][0] != -1:
        result.append(True)
    if storage_lst[1][0] == -1:
        result.append(False)
    if storage_lst[2][0] != -1:
        result.append(True)
    if storage_lst[2][0] == -1:
        result.append(False)
    return result


def merge(storage_lst, index_lst, mutex_lst):
    result = productoresRest(storage_lst)
    lista = []
    if result[0]:
        lista.append(storage_lst[0][0])
    if result[1]:
        lista.append(storage_lst[1][0])
    if result[2]:
        lista.append(storage_lst[2][0])
    if lista != []:
        minimo_value = min(lista)
    if minimo_value == storage_lst[0][0]:
        minimo_index = 0
    if minimo_value == storage_lst[1][0]:
        minimo_index = 1
    if minimo_value == storage_lst[2][0]:
        minimo_index = 2

    get_data(storage_lst[minimo_index], index_lst[minimo_index], mutex_lst[minimo_index])

    return minimo_value, minimo_index


def consumer(storage_lst, index_lst, empty_lst, non_empty_lst, mutex_lst, sorted_data_lst):
    
    for i in range(NPROD):
        non_empty_lst[i].acquire()
        result = productoresRest(storage_lst)
    i=0   
    while result:
        minimo_value, minimo_index = merge(storage_lst, index_lst, mutex_lst)
        print(f"consumer {current_process().name} deslmacenando")
        result = productoresRest(storage_lst)
        sorted_data_lst[i] = minimo_value
        empty_lst[minimo_index].release()
        non_empty_lst[minimo_index].acquire()
        print(f"consumer {current_process().name} consumiendo")
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
                       args=(storage_lst[i], index_lst[i], empty_lst[i], non_empty_lst[i], mutex_lst[i], sorted_data_lst[i])) for i in range(NCONS)]

    for p in prodlst + conslst:
        p.start()

    for p in prodlst + conslst:
        p.join()


if __name__ == '__main__':
    main()
