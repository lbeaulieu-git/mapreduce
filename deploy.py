import subprocess


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


# Réalisation d'un test de connexion aux machines du fichier machines.txt
def test_connexion():
    listproc = []
    timer = 5
    login = "lbeaulieu-20"

    print("Test de connexion aux machines : " + str(machines))

    for machine in machines:
        proc = subprocess.Popen(["ssh", login+"@"+machine, "hostname"],
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        listproc.append(proc)

    erreur_connexion = []
    for i in range(len(machines)):
        try:
            listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            if code != 0:
                erreur_connexion.append(machines[i])
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            erreur_connexion.append(machines[i])

    if len(erreur_connexion) == 0:
        print("Connexion aux machines OK")
    else:
        print("Connexion échouée aux machines : " + str(erreur_connexion))
        # for k in range(len(erreur_connexion)):
        #     del machines[machines.index(erreur_connexion[k])]

    # return machines


def dossiers(command):
    # Initialisation
    listproc = []
    timer = 5
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


def scp(localpath, distantpath):
    listproc = []
    timer = 5
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


test_connexion()
dossiers("/lbeaulieu-20/")
dossiers("/lbeaulieu-20/splits")
dossiers("/lbeaulieu-20/maps")
dossiers("/lbeaulieu-20/shuffles")
dossiers("/lbeaulieu-20/shufflesreceived")
dossiers("/lbeaulieu-20/reduces")
dossiers("/lbeaulieu-20/result")
dossiers("/lbeaulieu-20/wordcount")
scp("./slave.py", "/tmp/lbeaulieu-20/")
scp("./wordcount_single.py", "/tmp/lbeaulieu-20/")
