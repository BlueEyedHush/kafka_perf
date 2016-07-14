# todo: add analyzer part
# todo: currently we have shell scripts reimplemented; now add repeating of whole tests
# todo: use fabric.operations.get
# todo: add host aliases
# todo: redirect command outputs to coordinator
# todo: neither kafka's not zk's logs are downloaded

import ast
import datetime
import os
import random
import string
from fabric.api import *

env.shell = "/bin/bash -c"
env.always_use_pty = False

zk_jmx_port=9091
kafka_jmx_port=9092

class RemoteException(Exception):
    pass
env.abort_exception = RemoteException

base_app_dir = '/opt/kafka_perf'
base_data_dir = '/mnt/vol1'

kafka_dir = '{0}/kafka/latest'.format(base_app_dir)
kafka_data_dir = '{0}/kf'.format(base_data_dir)

zookeeper_dir = '{}/zookeeper/latest'.format(base_app_dir)

bench_dir = '{0}/bench'.format(base_app_dir)
python_sources_dir = '{0}/src/main/python'.format(bench_dir)

test_worker_jar = '{}/target/kafka_perf_test-0.1-jar-with-dependencies.jar'.format(bench_dir)

remote_log_directory = '/var/log/kafka_perf'
coordinator_log_path = './coordinator.out' # this file is stored remotelly, and then copied somewhere under local log dir
local_log_directory = './logs'
emergency_local_log_directory = '{}/emergency'.format(local_log_directory)

zookeeper_log_file = '/var/log/zookeeper/zookeeper.out'
kafka_log_file = '{}/logs/kafkaServer.out'.format(kafka_dir)
results_file_path = '/tmp/results'

def coord_log(msg):
    run('echo "[`date`]: {0}" >> {1}'.format(msg, coordinator_log_path))
    print msg

def get_random_string(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))

def download_file(host, from_path, to_path):
    os.system('scp {}:{} {} || true'.format(host, from_path, to_path))

def emergency_log_copy():
    for host in h('zk'):
        host_log_dir = '{}/{}'.format(emergency_local_log_directory, host)
        os.system('mkdir -p {}'.format(host_log_dir))
        download_file(host, zookeeper_log_file, host_log_dir)
        download_file(host, coordinator_log_path, host_log_dir)
        download_file(host, kafka_log_file, host_log_dir)


env.roledefs = {
    'all': ['128.142.128.88','128.142.134.233','188.184.165.208','128.142.242.119','128.142.134.55'],
    'kafka': ['128.142.128.88','128.142.134.233'],
    'zk': ['128.142.128.88','128.142.134.233','188.184.165.208'],
    'prod': ['188.184.165.208','128.142.242.119','128.142.134.55'],
    'zk_operator': ['188.184.165.208'], # node from which all commands to zk will be issued
    'prod_chosen': ['128.142.134.55'] # single node from prod group
}

def h(name):
    return env.roledefs[name]

@task
@parallel
@roles('all')
def init():
    run('echo LOG_START > {}'.format(coordinator_log_path))
    coord_log('creating local ({}) and remote ({}) log directories'.format(local_log_directory, remote_log_directory))
    run('rm -rf {}'.format(remote_log_directory))
    run('mkdir -p {0}'.format(remote_log_directory))
    # run('TMP_DIR=`mktemp -d` && '
    #     'mv {0}/* ${{TMP_DIR}} && '
    #     'echo "previous contents of log dir moved to ${{TMP_DIR}}"'.format(remote_log_directory))
    local('mkdir -p {}'.format(local_log_directory))
    coord_log('log directory created')

@task
@roles('kafka')
@parallel
def stop_kafka():
    coord_log('stopping kafka')
    run('''for pid in `ps aux | grep [k]afka.logs.dir | awk '{print $2}' | tr '\n' ' '`; do kill -s 9 $pid; done''')
    coord_log('kafka_stopped')

@task
@parallel
@roles('zk')
def ensure_zk_running():
    zk_server_script_path = '{}/bin/zkServer.sh'.format(zookeeper_dir)
    coord_log('ensuring that zk is running & truncating log')
    run('echo ----trunc---- > {}'.format(zookeeper_log_file))
    run('export ZOO_LOG_DIR={0} && export JMXPORT={1} && {2} start'.format(remote_log_directory, zk_jmx_port, zk_server_script_path))
    coord_log('zk should be running')

@task
@roles('zk_operator')
def purge_zookeeper():
    purge_script_path = '{0}/zkDelAll.py'.format(python_sources_dir)
    coord_log('removing all znodes except /zookeeper')
    run('''python {0} /'''.format(purge_script_path))
    coord_log('purging complete')

@task
@parallel
@roles('kafka')
def cleanup_after_kafka():
    coord_log('emptying kafka log directories')
    run('rm -rf {0}'.format(kafka_data_dir))
    run('mkdir -p {0}', kafka_data_dir)
    coord_log('kafka log directories emptied')

@task
@parallel
@roles('kafka')
def remove_kafka_log():
    run('rm -f {}'.format(kafka_log_file))

@task
@parallel
@roles('prod')
def remove_result_files():
    run('rm -rf {}'.format(results_file_path))

@task
@parallel
@roles('kafka')
def ensure_kafka_running():
    kafka_start_script_path = '{0}/bin/kafka-server-start.sh'.format(kafka_dir)
    kafka_config_path = '{0}/config/server.properties'.format(kafka_dir)
    coord_log('ensuring kafka is running')
    run('export JMX_PORT={0} && {1} -daemon {2}'.format(kafka_jmx_port, kafka_start_script_path, kafka_config_path))
    coord_log('kafka should be running')

@task
@roles('prod_chosen')
def create_topics(number_of_topics):
    coord_log('creating topics')
    run('java -jar {} -T {} -c'.format(test_worker_jar, number_of_topics))
    coord_log('topics created')

@task
@parallel
@roles('prod')
def run_test(series, reps, threads, topics):
    coord_log('executing actual test')
    run('java -jar {0} -s {1} -r {2} -t {3} -T {4} > {5}'.format(test_worker_jar, series, reps, threads, topics, results_file_path))
    coord_log('test execution finished')

@task
@parallel
@roles('all')
def log_test_set_execution_start(set_name, series, reps, threads, topics):
    coord_log('starting test set {} (series = {}, reps = {}, threads = {}, topics = {})'
              .format(set_name, series, reps, threads, topics))

@task
@parallel
@roles('all')
def log_test_set_execution_end(set_name):
    coord_log('test set {} finished'.format(set_name))

@task
def run_test_set(suite_name, set_name, series, reps, threads, topics):
    execute(log_test_set_execution_start, set_name, series, reps, threads, topics)

    execute(stop_kafka)
    execute(cleanup_after_kafka)
    execute(remove_kafka_log)
    execute(purge_zookeeper)
    execute(ensure_kafka_running)
    execute(remove_result_files)
    execute(create_topics, topics)

    execute(run_test, series, reps, threads, topics)

    for host in h('prod'):
        current_log_dir = "{}/{}/{}/{}".format(local_log_directory, suite_name, set_name, host)
        local('mkdir -p {}'.format(current_log_dir))
        download_file(host, results_file_path, current_log_dir)
        download_file(host, kafka_log_file, current_log_dir)

    execute(log_test_set_execution_end, suite_name)

@task
@runs_once
def run_test_suite(topics='[1]', series=1, reps=10, threads=2):
    suite_name = datetime.datetime.now().strftime('%H%M_%d%m%y')
    suite_log_dir = "{}/{}".format(local_log_directory, suite_name)
    local('mkdir -p {}'.format(suite_log_dir))

    try:
        execute(init)
        execute(ensure_zk_running)

        topic_progression = ast.literal_eval(topics)
        for i in range(0, len(topic_progression)):
            for j in range(0, int(series)):
                set_name = 't{}_{}'.format(str(topic_progression[i]), str(j))
                execute(run_test_set, suite_name, set_name, 1, reps, threads, topic_progression[i])

        for host in h('zk'):
            local_dir = '{}/{}/persuite/{}'.format(local_log_directory, suite_name, host)
            local('mkdir -p {}'.format(local_dir))
            download_file(host, zookeeper_log_file, local_dir)
            download_file(host, coordinator_log_path, local_dir)
    finally:
        emergency_log_copy()



