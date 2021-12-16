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


def clean(x):
    login = "lbeaulieu-20"
    m_master = 'tp-4b01-34'
    listproc = []
    timer = 5



    if x == 1:
        for machine in machines:
            if machine != m_master:
                proc = subprocess.Popen(["ssh", login + "@" + machine, "rm -rf /tmp/lbeaulieu-20/"],
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
                listproc.append(proc)
            else:
                command = "rm -f /tmp/lbeaulieu-20/splits/*" \
                          "; rm -f /tmp/lbeaulieu-20/maps/* " \
                          "; rm -f /tmp/lbeaulieu-20/shuffles/*" \
                          "; rm -f /tmp/lbeaulieu-20/shufflesreceived/*" \
                          "; rm -f /tmp/lbeaulieu-20/reduces/*" \
                          "; rm -f /tmp/lbeaulieu-20/result/*" \
                          "; rm -f /tmp/lbeaulieu-20/wordcount/*" \
                          "; rm -f /tmp/lbeaulieu-20/wordcount.txt" \
                          "; rm -f /tmp/lbeaulieu-20/wordcount_single.txt"

                proc = subprocess.Popen(["ssh", login + "@" + machine, command],
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
                listproc.append(proc)

        for i in range(len(machines)):
            try:
                out, err = listproc[i].communicate(timeout=timer)
                code = listproc[i].returncode
                print(str(i) + " out: '{}'".format(out))
                print(str(i) + " err: '{}'".format(err))
                print(str(i) + " exit: {}".format(code))
            except subprocess.TimeoutExpired:
                listproc[i].kill()
                print(str(i) + " timeout")


clean(1)
