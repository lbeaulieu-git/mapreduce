import sys
from time import time


def main():
    start_time = time()
    file = sys.argv[1]

    with open(file) as f:
        text = f.read()
    words = text.split()
    dico = {}
    for word in words:
        if word in dico:
            dico[word] += 1
        else:
            dico[word] = 1
    open('wordcount_single.txt', 'w')
    with open('wordcount_single.txt', 'a') as wc:
        for cle, valeur in dico.items():
            wc.write(cle + " " + str(valeur) + "\n")

    print(time() - start_time)


if __name__ == '__main__':
    main()
