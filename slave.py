import sys
import hashlib
import subprocess
from os import walk
import random
from time import time


def choix_machines(nn, NN):
    datacenter = 'tp-4b01-'
    # machines = [item.split('\n')[0] for item in open('machines.txt').readlines()]
    machines = ['tp-4b01-34']
    for k in range(nn, NN):
        if k != 34:
            if k < 10:
                machines.append(datacenter + "0" + str(k))
            else:
                machines.append(datacenter + str(k))
    # liste_machines_f = test_connexion(machines)
    return machines


machines = choix_machines(24, 40)


# Fonction de récupération du hostname de la machine
def hostname():
    timer = 5
    out, err = subprocess.Popen(["hostname"],
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE).communicate(timeout=timer)
    return out.decode('utf-8').split('\n')[0]


# Fonction de déplacement d'un fichier sur la même machine
def mv(localpath, distantpath):
    timer = 5

    copy = subprocess.Popen(["mv", localpath, distantpath],
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

    try:
        print("Copie du fichier " + localpath + " dans le répertoire " + distantpath)
        out, err = copy.communicate(timeout=timer)
        code = copy.returncode
        if err != b'':
            print("err: '{}'".format(err))
            print("exit: {}".format(code))
    except subprocess.TimeoutExpired:
        copy.kill()
        print("timeout")


# Fonction de déplacement d'un fichier sur une autre machine
def scp(localpath, machine, distantpath):
    timer = 5
    login = "lbeaulieu-20"

    copy = subprocess.Popen(["scp", localpath, login+"@"+machine+":"+distantpath],
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

    try:
        print("Copie du fichier " + localpath + " dans le répertoire " + distantpath
              + " de la machine " + machine)
        out, err = copy.communicate(timeout=timer)
        code = copy.returncode
        if err != b'':
            print("err: '{}'".format(err))
            print("exit: {}".format(code))
    except subprocess.TimeoutExpired:
        copy.kill()
        print("timeout")


def main():

    x = sys.argv[1]
    file = sys.argv[2]

    # Création des fichiers de split à partir de input.wet
    if x == str(99):
        with open(file, 'r') as f:
            ff = f.readlines()
            size = len(ff)
            nb_files = len(machines)
            size_goal = size / nb_files

            if size % nb_files == 0:
                k = 0
                for i in range(nb_files):
                    with open("/tmp/lbeaulieu-20/splits/S{}.txt".format(i), 'w'):
                        cible = open("/tmp/lbeaulieu-20/splits/S{}.txt".format(i), 'a')
                        for j in range(k, k + int(size_goal)):
                            cible.write(ff[j])
                        cible.close()
                        k += size_goal
            else:
                k = 0
                for i in range(nb_files - 1):
                    with open("/tmp/lbeaulieu-20/splits/S{}.txt".format(i), 'w'):
                        cible = open("/tmp/lbeaulieu-20/splits/S{}.txt".format(i), 'a')
                        for j in range(k, k + int(size_goal)):
                            cible.write(ff[j])
                        cible.close()
                        k += int(size_goal)
                with open("/tmp/lbeaulieu-20/splits/S{}.txt".format(nb_files - 1), 'w'):
                    cible = open("/tmp/lbeaulieu-20/splits/S{}.txt".format(nb_files - 1), 'a')
                    for j in range(k, k + int(size_goal) + 1):
                        cible.write(ff[j])
                    cible.close()

    # Création des maps
    if x == str(0):
        with open(file) as f:
            text = f.read()
        words = text.split()
        name = "UM{}.txt".format(file[-5])
        open("/tmp/lbeaulieu-20/maps/" + name, 'w')
        cible = open("/tmp/lbeaulieu-20/maps/" + name, 'a')
        for word in words:
            cible.write(word.lower() + " 1\n")
        cible.close()

    # Création des shuffles
    if x == str(1):
        start_time2 = time()
        # Création d'une liste de log
        log = []

        # Initialisation
        liste_cles = []

        t_1 = time()-start_time2
        log.append("Initialisation : " + str(t_1) + "\n")

        # Ouverture des maps et listing des mots contenus dans chaque fichier UMx.txt
        map_file = open(file, "r")
        lignes = map_file.readlines()
        for ligne in lignes:
            liste_cles.append(ligne.split(" ")[0])
        map_file.close()

        t_2 = time()-start_time2-t_1
        log.append("Ouverture des maps et listing des mots contenus dans chaque fichier UMx.txt : " + str(t_2) + "\n")

        # Création d'un zip clé - hash - machine cible
        def cle_hash(cle):
            hashh = str(int.from_bytes(hashlib.sha256(cle.encode('UTF-8')).digest()[:4], 'little'))
            return hashh
        liste_hash = [cle_hash(liste_cles[i]) for i in range(len(liste_cles))]
        liste_machines = [int(float(item) % len(machines)) for item in liste_hash]
        zipp = list(zip(liste_cles, liste_hash, liste_machines))

        t_3 = time() - start_time2 - t_1 - t_2
        log.append("Création d'un dataframe clé - hash - machine cible : " + str(t_3) + "\n")

        # Création et déplacement des fichiers shuffle
        for k in range(len(machines)):

            # Création du fichier shuffle
            start_time3 = time()
            words = [j[0] for j in zipp if j[2] == k]
            name_shuffle = "/tmp/lbeaulieu-20/shuffles/" + str(k) + "-" + hostname() + ".txt"
            open(name_shuffle, 'w')
            cible = open(name_shuffle, 'a')
            for word in words:
                cible.write(word + "\n")
            cible.close()

            t_41 = time() - start_time3
            log.append(str(k) + "/" + str(len(machines) - 1) +
                       " Création du fichier shuffle {} : ".format(str(k)) + str(t_41) + "\n")

            # Envoi sur une autre machine ou mv si même machine
            if machines[k] == hostname():
                mv(name_shuffle, "/tmp/lbeaulieu-20/shufflesreceived/")
            else:
                scp(name_shuffle, machines[k], "/tmp/lbeaulieu-20/shufflesreceived/")

            t_42 = time() - start_time3 - t_41
            log.append(str(k) + "/" + str(len(machines)-1) +
                       " Envoi sur une autre machine ou mv si même machine {} : ".format(str(k)) + str(t_42) + "\n")

        t_4 = time() - start_time2 - t_1 - t_2 - t_3
        log.append("Création et déplacement des fichiers shuffle : " + str(t_4) + "\n")

        open('/tmp/lbeaulieu-20/log.txt', 'w')
        with open('/tmp/lbeaulieu-20/log.txt', 'a') as f:
            for i in range(len(log)):
                f.write(log[i])

    # Phase de reduce
    if x == str(2):
        # Concaténation des fichiers reçus
        liste_fichiers = [item for item in next(walk(file), (None, None, []))[2]]
        liste_concat = []
        for fichier in liste_fichiers:
            name = "/tmp/lbeaulieu-20/shufflesreceived/" + fichier
            with open(name) as fp:
                data = fp.read()
                liste_concat.append(data)
        liste_concat[0] += "\n"  # voir si utile
        for k in range(len(liste_concat)-1):
            liste_concat[0] += liste_concat[k+1]
        n = random.randint(1, 9999999)
        with open('/tmp/lbeaulieu-20/result/shuf_rec{}.txt'.format(n), 'w') as fp:
            fp.write(liste_concat[0])

    # Wordcount
    if x == str(3):
        with open(file) as f:
            text = f.read()
        words = text.split()
        dico = {}
        for word in words:
            if word in dico:
                dico[word] += 1
            else:
                dico[word] = 1
        with open('/tmp/lbeaulieu-20/wordcount/wordcount-{}.txt'.format(hostname()), 'w') as f:
            for cle, valeur in dico.items():
                f.write(cle + " " + str(valeur) + '\n')

    # Concanténation des wordcount sur la machine master
    if x == str(4):
        liste_fichiers = [item for item in next(walk(file), (None, None, []))[2]]
        liste_concat = []
        for fichier in liste_fichiers:
            name = "/tmp/lbeaulieu-20/wordcount/" + fichier
            with open(name) as fp:
                data = fp.read()
                liste_concat.append(data)
        liste_concat[0] += "\n"  # voir si utile
        for k in range(len(liste_concat)-1):
            liste_concat[0] += liste_concat[k+1]
        with open('/tmp/lbeaulieu-20/wordcount.txt', 'w') as fp:
            fp.write(liste_concat[0])


if __name__ == '__main__':
    main()
