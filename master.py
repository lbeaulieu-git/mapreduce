import subprocess
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


def scp_3_txt(localpath, m_master):
    timer = 120
    login = "lbeaulieu-20"
    files = []

    for i in range(len(machines)):
        files.append("S{}.txt".format(i))

    print(m_master + " : copie des fichiers txt dans le répertoire /splits/")
    for file in files:
        proc = subprocess.Popen(["scp", localpath + file, login+"@"+m_master+":/tmp/lbeaulieu-20/splits"],
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

        try:

            out, err = proc.communicate(timeout=timer)
            code = proc.returncode
            if err != b'':
                print(m_master + " err: '{}'".format(err))
                print(m_master + " exit: {}".format(code))
        except subprocess.TimeoutExpired:
            proc.kill()
            print("timeout")


# Vérification d'existence et création éventuelle d'un répertoire (racine : dossier /tmp)
def dossiers(command):
    # Initialisation
    listproc = []
    timer = 10
    login = "lbeaulieu-20"

    for machine in machines:
        proc = subprocess.Popen(["ssh", login + "@" + machine, "cd /tmp" + command],
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        listproc.append(proc)

    # Vérification de l'existence du répertoire et création éventuelle
    for i in range(len(machines)):
        try:
            listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            if code != 0:
                print(machines[i] + " : création du dossier " + command + " dans le répertoire /tmp/")
                subprocess.Popen(["ssh", login + "@" + machines[i], "mkdir /tmp" + command]).communicate(timeout=timer)
            else:
                print(machines[i] + " : le dossier " + command + " existe déjà dans le répertoire /tmp/")

        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i) + " timeout")


# Fonction générique de copie d'un fichier en local vers un répertoire distant
# sur toutes les machines du fichier machines.txt
def scp(localpath, distantpath):
    listproc = []
    timer = 120
    login = "lbeaulieu-20"

    for machine in machines:
        proc = subprocess.Popen(["scp", localpath, login+"@"+machine+":"+distantpath],
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        listproc.append(proc)

    for i in range(len(machines)):
        try:
            print(machines[i] + " : copie du fichier " + localpath + " dans le répertoire " + distantpath)
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            if err != b'':
                print(str(i)+" err: '{}'".format(err))
                print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")


# Copie des fichiers de splits depuis la machine master vers les autres machines (1 par machine)
def copie_fichiers_split(m_master):
    login = "lbeaulieu-20"
    timer = 120

    # Liste des fichiers split présents dans le dossier /tmp/lbeaulieu-20/splits de la machine master
    proc = subprocess.Popen(["ssh", login + "@" + m_master, "ls /tmp/lbeaulieu-20/splits/"],
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    try:
        out, err = proc.communicate(timeout=timer)
        code = proc.returncode
        liste_fichiers = str(out)[2:].split('\\n')[0:str(out).count('\\n')]
        if len(liste_fichiers) == len(machines):
            print("Copie des fichiers : " + str(liste_fichiers))

            # Copie des fichiers
            k = 1
            for machine in machines:
                if machine != m_master:
                    copy = subprocess.Popen(["scp",
                                             login + "@" + m_master + ":/tmp/lbeaulieu-20/splits/" + liste_fichiers[k],
                                             machine + ":/tmp/lbeaulieu-20/splits/"],
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE)
                    try:
                        print(machine + " : copie du fichier " + liste_fichiers[k] + " dans le répertoire /splits/")
                        out, err = copy.communicate(timeout=timer)
                        code = copy.returncode
                        if err != b'':
                            print(machine + " err: '{}'".format(err))
                            print(machine + " exit: {}".format(code))
                    except subprocess.TimeoutExpired:
                        copy.kill()
                        print("timeout")
                    k += 1

            # Suppression des fichiers copiés de la machine master
            print(m_master + " : suppression des fichiers copiés")
            for k in range(1, len(liste_fichiers)):
                supp = subprocess.Popen(["ssh",
                                         login + "@" + m_master,
                                         "rm -f /tmp/lbeaulieu-20/splits/" + liste_fichiers[k]],
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
                try:
                    out, err = supp.communicate(timeout=timer)
                    code = supp.returncode
                    if err != b'':
                        print(m_master + " err: '{}'".format(err))
                        print(m_master + " exit: {}".format(code))
                except subprocess.TimeoutExpired:
                    supp.kill()
                    print("timeout")

            if err != b'':
                print("err: '{}'".format(err))
                print("exit: {}".format(code))
        else:
            print("Le nombre de fichiers ne correspond pas au nombre de machines")
    except subprocess.TimeoutExpired:
        proc.kill()
        print("timeout")


# Fonction générique de copie d'un fichier vers la machine master
def scp_wc(distantpath, localpath, m_master):
    listproc = []
    timer = 120
    login = "lbeaulieu-20"

    for i in range(len(machines)):
        if machines[i] == m_master:
            del machines[i]
            break

    for machine in machines:
        proc = subprocess.Popen(["scp", login+"@"+machine+":"+distantpath, login+"@"+m_master+":"+localpath],
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        listproc.append(proc)

    for i in range(len(machines)):
        try:
            print(machines[i] + " : copie des fichiers result.txt")
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            if err != b'':
                print(str(i)+" err: '{}'".format(err))
                print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")


# Commande ssh générique sur toutes les machines du fichier machines.txt
def ssh(command, timer, step):
    # Initialisation
    error = 0
    listproc = []
    login = "lbeaulieu-20"


    for machine in machines:
        proc = subprocess.Popen(["ssh", login + "@" + machine, command],
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        listproc.append(proc)

    # Vérification de l'existence du répertoire et création éventuelle
    for i in range(len(machines)):
        try:
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            print(machines[i] + " : " + step + " FINISHED")
            if err != b'':
                print(machines[i] + " err: '{}'".format(err))
                print(machines[i] + " exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(machines[i] + " : " + step + " timeout")
            error = 1
            break
    return error


def main():
    start_time = time()

    print("Début du programme...")

    process = subprocess.Popen(["ssh",
                                "lbeaulieu-20@tp-4b01-34",
                                "python3.7 /tmp/lbeaulieu-20/slave.py 99 /tmp/lbeaulieu-20/file_1.5G.wet"],
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    try:
        out, err = process.communicate(timeout=120)
        code = process.returncode
        print("SPLITS OK")
        if err != b'':
            print("Splits err: '{}'".format(err))
            print("Splits exit: {}".format(code))
    except subprocess.TimeoutExpired:
        process.kill()
        print("timeout")

    # Copie des fichiers splits depuis la machine master vers les autres machines
    copie_fichiers_split('tp-4b01-34')
    t_split = round(time()-start_time, 5)

    # Création des fichiers maps UMx.txt à partir des fichiers splits Sx.txt (1 par machine)
    ssh("python3.7 /tmp/lbeaulieu-20/slave.py 0 /tmp/lbeaulieu-20/splits/*", 120, "MAP")
    t_map = round(time()-start_time-t_split, 5)

    t_shuffle = 0
    t_reduce = 0
    t_wc = 0
    if ssh("python3.7 /tmp/lbeaulieu-20/slave.py 1 /tmp/lbeaulieu-20/maps/*", 300, "SHUFFLE") == 0:
        t_shuffle = round(time() - start_time - t_split - t_map, 5)

        # Phase de reduce
        ssh("python3.7 /tmp/lbeaulieu-20/slave.py 2 /tmp/lbeaulieu-20/shufflesreceived/", 120, "REDUCE")
        t_reduce = round(time()-start_time-t_split-t_map-t_shuffle, 5)

        # wordcount
        ssh("python3.7 /tmp/lbeaulieu-20/slave.py 3 /tmp/lbeaulieu-20/result/*", 120, "WORDCOUNT")

        # Copie des fichiers de wordcount sur la machine master et concanténation
        scp_wc("/tmp/lbeaulieu-20/wordcount/*", "/tmp/lbeaulieu-20/wordcount", "tp-4b01-34")
        process = subprocess.Popen(["ssh",
                                    "lbeaulieu-20@tp-4b01-34",
                                    "python3.7 /tmp/lbeaulieu-20/slave.py 4 /tmp/lbeaulieu-20/wordcount/"],
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        try:
            out, err = process.communicate(timeout=120)
            code = process.returncode
            print("WORDCOUNT FINISHED")
            if err != b'':
                print("Wordcount err: '{}'".format(err))
                print("Wordcount exit: {}".format(code))
        except subprocess.TimeoutExpired:
            process.kill()
            print("timeout")

        t_wc = round(time() - start_time - t_split - t_map - t_shuffle - t_reduce, 5)

    t_total = round(time()-start_time, 5)

    print("Split : " + str(t_split))
    print("Map : " + str(t_map))
    print("Shuffle : " + str(t_shuffle))
    print("Reduce : " + str(t_reduce))
    print("Wordcount : " + str(t_wc))
    print("-----------")
    print("TOTAL : " + str(t_total))
    print("-----------")

    TEMPS = str(t_split) + "," + str(t_map) + "," + str(t_shuffle) \
        + "," + str(t_reduce) + "," + str(t_wc) + "," + str(t_total) + "\n"

    print(TEMPS)


if __name__ == '__main__':
    main()
