#Autores: Marcos Esteve Hernández y Alberto Blanco Álvarez

import numpy as np
import pywren_ibm_cloud as pywren
import pickle
import time
import random

Bucket = 'bucketsistemasdistrib'
iterdata = []


def iniMatrix(workers, n_fil_mA, n_col_mA, n_fil_mB, n_col_mB, ibm_cos):
    #inicializar las matrices, y segun el numero de workers, dividirlas en submatrices y subirlas al COS (put.object)
    np.random.seed(random.randint(1,1200))
    #matrizA = np.random.randint(10, size=(n_fil_mA, n_col_mA)) 
    matrizA =np.array([[2,3,3],[4,7,3],[1,2,6]])
    #matrizB = np.random.randint(10, size=(n_fil_mB, n_col_mB))
    matrizB = np.array([[4,5,7],[7,8,3],[5,1,3]])
    fil_x_work = 1
    col_x_work = 1
    filas = []
    columnas = []

    #inicializamos las filas y columnas que tendrá que ejecutar cada worker
    if (workers <= n_fil_mA):               
        fil_x_work = int(n_fil_mA / workers)
        col_x_work = n_col_mB
    
    #si el numero de workers es menor al de filas, dejamos que el ultimo worker se encargue de las filas restantes
    if  (n_fil_mA % workers != 0 and workers < n_fil_mA):  
        fil_ult_work = fil_x_work + (n_fil_mA%workers)
        n_fil_mA = n_fil_mA - fil_ult_work
        
    #subdividir la matriz A
    for i in range(0, n_fil_mA, fil_x_work): 
        subA = matrizA[i:i+fil_x_work,:]
        ready=0
        ready = pickle.dumps(subA)
        ibm_cos.put_object(Bucket=Bucket, Key='SubA'+str(i)+'.txt', Body=ready)
        filas.append('SubA'+str(i)+'.txt')

    #ultima fila en el caso que los workers sean menos que las filas
    if( n_fil_mA % workers != 0 and workers < n_fil_mA):
        subA = matrizA[n_fil_mA:n_fil_mA+fil_ult_work,:]
        ready = pickle.dumps(subA)
        ibm_cos.put_object(Bucket=Bucket, Key='SubA'+str(n_fil_mA)+'.txt', Body=ready)
        filas.append('SubA'+str(n_fil_mA)+'.txt')

    #subdividir la matriz B
    for i in range(0, n_col_mB, col_x_work): 
        subB = matrizB[:,i:i+col_x_work]
        ready = pickle.dumps(subB)
        ibm_cos.put_object(Bucket=Bucket, Key='SubB'+str(i)+'.txt', Body=ready)
        columnas.append('SubB'+str(i)+'.txt')

    #retornaremos un vector en el que cada elemento será las submatrices que se han de multiplicar, separadas por un *
    iterdata = []
    for i in range (len(filas)):
        for j in range (len(columnas)):
            iterdata.append(filas[i]+"*"+columnas[j])
    return iterdata


def my_map_function(result, ibm_cos): 
    #descargamos las matrices que toquen, las multiplicamos y guardamos el resultado a modo de diccionario
    Ficheros = result.split("*") 
    subA = Ficheros[0]
    subB = Ficheros[1]
    FichA = ibm_cos.get_object(Bucket=Bucket, Key=subA)['Body'].read()
    FichB = ibm_cos.get_object(Bucket=Bucket, Key=subB)['Body'].read()

    MatA = pickle.loads(FichA)
    MatB = pickle.loads(FichB)
    
    res = MatA.dot(MatB)

    if (subA[5]!='.'): idA = subA[4] + subA[5] + subA[3]
    else: idA = subA[4] + subA[3]
    if (subB[5]!='.'): idB = subB[4] + subB[5] + subB[3]
    else: idB = subB[4] + subB[3]
    idDic = idA + idB             #La clave del diccionario sera del formato submatriz a sumbatriz b
    return {idDic: res.tolist()}  #Retornamos un diccionario

def my_reduce_function(results):
    dicc = {}
    
    for map_result in results:
        dicc.update(map_result)

    matr = []
    for i in dicc:
       matr.extend(dicc[i])

    global  n_col_mB, n_fil_mA, workers
    if (workers== n_fil_mA*n_col_mB):
        aux_mat = []
        final_mat = [0] * n_fil_mA
        i=0
        k=0
        j=0
        while(i<len(matr)):
            while(k<n_col_mB):
                aux_mat.extend(matr[i])
                i=i+1
                k=k+1
            k=0
            final_mat[j] = aux_mat
            j=j+1
            aux_mat = []
        
        matr=final_mat

    return matr

if __name__ == '__main__':
    
    n_fil_mA = 3
    n_col_mA = 3
    n_fil_mB = 3
    n_col_mB = 3
    workers = 1


    if (workers>100): workers=100

    if (n_col_mA == n_fil_mB): 
        
        if((workers != n_fil_mA*n_col_mB) and (workers > n_fil_mA)): #si no se cumplen estas condiciones, hacer secuencial
            print("\033[1;32;40m La ejecución se ha cambiado a secuencial porque el número de workers no era correcto")
            print("\033[1;0;0m")
            workers = 1
        
        pw = pywren.ibm_cf_executor()
        start_time = time.time()
        pw.call_async(iniMatrix, [workers, n_fil_mA, n_col_mA, n_fil_mB, n_col_mB])
        iterdata=pw.get_result()
        print(iterdata)
        
        matrix_final = pw.map_reduce(my_map_function, iterdata, my_reduce_function)
        pw.wait(matrix_final)
        elapsed_time = time.time() - start_time
        print(pw.get_result())
        print ('Tiempo: {0:.2f} segundos'.format(elapsed_time))
    else:
        print('Error, la multiplicacion no es posible')