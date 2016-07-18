# todo: add analyzer part
# todo: use fabric.operations.get
# todo: add host aliases
# todo: redirect command outputs to coordinator

import ast
import datetime
import os
import random
import string
from fabric.api import *

env.shell = "/bin/bash -c"
env.always_use_pty = False
env.use_ssh_config = True

zk_jmx_port=9091
kafka_jmx_port=9093

class RemoteException(Exception):
    pass
env.abort_exception = RemoteException

# paths on local machine
local_python_dir='./src/main/python'
orchestrator_script_path='{}/orchestrator.py'.format(local_python_dir)
analyzer_script_path = '{}/analyzer.py'.format(local_python_dir)
local_log_directory = './logs' # downloaded logs and results are stored there
emergency_local_log_directory = '{}/emergency'.format(local_log_directory) # in case of serious failure during
                                                                           # execution logs will be copied here
# paths on remote machine
bench_dir = '/opt/kafka_perf/bench'
bundle_dir = '/data1/cals/kafka_perf/bundle'
data_dir = '/data1/cals/kafka_perf/data'

kafka_dir = '{}/kafka/latest'.format(bundle_dir)
zookeeper_dir = '{}/zookeeper/latest'.format(bundle_dir)
kafka_data_dir = '{}/kf'.format(data_dir)

python_sources_dir = '{}/src/main/python'.format(bench_dir)
test_worker_jar = '{}/target/kafka_perf_test-0.2-jar-with-dependencies.jar'.format(bench_dir)

remote_log_directory = '~/log/kafka_perf'
coordinator_log_path = '{}/coordinator.out'.format(remote_log_directory) # this file is stored remotelly,
                                                                    # and then copied somewhere under local log dir
bench_service_log_path = '{}/bench.out'.format(remote_log_directory)
zookeeper_log_file = '{}/zookeeper.out'
kafka_log_file = '{}/logs/kafkaServer.out'.format(kafka_dir)
results_file_path = '/tmp/results'

# aliases
a = {
    'i9': 'itrac1509',
    'i10': 'itrac1510',
    'i11': 'itrac1511',
    'i12': 'itrac1512',
    'o1': 'cals-kafka-perf-bf4cbe5f-709f-4158-b1de-f49e3d0dfeef',
    'o2': 'cals-kafka-perf-df4766f8-d34a-406c-b751-8372efcdde22',
    'o3': 'cals-kafka-perf-9eb4ca80-8369-496f-b455-9c25d6ad4a8b',
    'o4': 'cals-kafka-perf-bbcc82bc-1943-4297-bb48-d6b536ad83e4',
    'o5': 'cals-kafka-perf-f61d20da-c57c-4536-87f5-330b2ded8b74',
    'o6': 'cals-kafka-perf-a6fd3630-4503-474a-a11c-f968f6d70f03',
    'o7': 'cals-kafka-perf-4dc7e768-ccce-4f6e-9c6f-eccc6082ee2c',
    'o8': 'cals-kafka-perf-0903fdc7-1269-4e9f-8edc-1f526b42da04',
}

# groups of hosts
env.roledefs = {
    'all': a.values(),
    'kafka': [a['i9'], a['i10']],
    'zk': [a['i12']],
    'prod': [a['o1'], a['o2'], a['o3']],
    'zk_operator': [a['o8']], # node from which all commands to zk will be issued
    'prod_chosen': [a['o1']] # single node from prod group
}

def coord_log(msg):
    run('echo "[`date`]: {0}" >> {1}'.format(msg, coordinator_log_path))
    print msg

def get_random_string(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))

def download_file_error(host, from_path, to_path):
    os.system('scp {}:{} {}'.format(host, from_path, to_path))

def download_file(host, from_path, to_path):
    os.system('scp {}:{} {} || true'.format(host, from_path, to_path))

def emergency_log_copy():
    for host in h('zk'):
        host_log_dir = '{}/{}'.format(emergency_local_log_directory, host)
        os.system('mkdir -p {}'.format(host_log_dir))
        download_file(host, zookeeper_log_file, host_log_dir)
        download_file(host, coordinator_log_path, host_log_dir)
        download_file(host, kafka_log_file, host_log_dir)

    for host in h('prod'):
        host_log_dir = '{}/{}'.format(emergency_local_log_directory, host)
        os.system('mkdir -p {}'.format(host_log_dir))
        download_file(host, bench_service_log_path, host_log_dir)

def h(name):
    return env.roledefs[name]

def run_with_logging(command):
    run('{} 2>&1 | tee -a {}'.format(command, coordinator_log_path))

def run_daemonized(cmd, log_path):
    run('nohup {0} > {1} 2>&1 < /dev/null &'.format(cmd, log_path))

@task
@roles('all')
def init():
    run('mkdir -p {0}'.format(remote_log_directory))
    run('echo ---------`date`--------- > {}'.format(coordinator_log_path))
    local('mkdir -p {}'.format(local_log_directory))
    coord_log('log directory created')

@task
@roles('kafka')
@parallel
def stop_kafka():
    coord_log('stopping kafka')
    run_with_logging('''for pid in `ps aux | grep [k]afka.logs.dir | awk '{print $2}' | tr '\n' ' '`; do kill -s 9 $pid; done''')
    coord_log('kafka_stopped')

@task
@parallel
@roles('prod')
def restart_benchmark_daemons(threads):
    coord_log('stopping testing daemons')
    run_with_logging('''for pid in `ps aux | grep [k]afka_perf_test | awk '{print $2}' | tr '\n' ' '`; do kill -s 9 $pid; done''')
    run('mv {} /tmp || true'.format(bench_service_log_path))
    coord_log('starting testing daemons')
    run_daemonized('java -jar {} -t {}'.format(test_worker_jar, threads), bench_service_log_path)

@task
@parallel
@roles('zk')
def ensure_zk_running():
    zk_server_script_path = '{}/bin/zkServer.sh'.format(zookeeper_dir)
    coord_log('ensuring that zk is running & truncating log')
    run('echo ----trunc---- > {}'.format(zookeeper_log_file))
    run_with_logging('export ZOO_LOG_DIR={0} && export JMXPORT={1} && {2} start'
                     .format(remote_log_directory, zk_jmx_port, zk_server_script_path))
    coord_log('zk should be running')

@task
@roles('zk_operator')
def purge_zookeeper():
    purge_script_path = '{0}/zkDelAll.py'.format(python_sources_dir)
    coord_log('removing all znodes except /zookeeper and /kafka_perf_test')
    run_with_logging('python {0} /'''.format(purge_script_path))
    coord_log('purging complete')

@task
@parallel
@roles('kafka')
def cleanup_after_kafka():
    coord_log('emptying kafka log directories')
    run_with_logging('rm -rf {0}'.format(kafka_data_dir))
    run_with_logging('mkdir -p {0}'.format(kafka_data_dir))
    coord_log('kafka log directories emptied')

@task
@parallel
@roles('kafka')
def remove_kafka_log():
    run_with_logging('rm -f {}'.format(kafka_log_file))

@task
@parallel
@roles('prod')
def remove_result_files():
    run_with_logging('rm -rf {}'.format(results_file_path))

@task
@parallel
@roles('kafka')
def ensure_kafka_running():
    kafka_start_script_path = '{0}/bin/kafka-server-start.sh'.format(kafka_dir)
    kafka_config_path = '{0}/config/server.properties'.format(kafka_dir)
    coord_log('ensuring kafka is running')
    run_with_logging('export JMX_PORT={0} && {1} -daemon {2}'
                     .format(kafka_jmx_port, kafka_start_script_path, kafka_config_path))
    coord_log('kafka should be running')

@task
@roles('prod_chosen')
def create_topics(number_of_topics):
    coord_log('creating topics')
    run_with_logging('java -jar {} -T {} -c'.format(test_worker_jar, number_of_topics))
    coord_log('topics created')

@task
@parallel
@roles('all')
def log_actual_testing_started():
    coord_log('actual testing started')

def run_test(duration, message_size, topics):
    execute(log_actual_testing_started)
    local('python {} -d {} -s {} -t {}'.format(orchestrator_script_path, duration, message_size, topics))
    local('sleep 5s')

@task
@parallel
@roles('all')
def log_test_set_execution_start(set_name, duration, message_size, topics):
    coord_log('starting test set {} (duration = {}, message_size = {}, topics = {})'
              .format(set_name, duration, message_size, topics))

@task
@parallel
@roles('all')
def log_test_set_execution_end(set_name):
    coord_log('test set {} finished'.format(set_name))

@task
def run_test_set(suite_name, set_name, duration, message_size, topics):
    execute(log_test_set_execution_start, set_name, duration, message_size, topics)

    execute(stop_kafka)
    execute(cleanup_after_kafka)
    execute(remove_kafka_log)
    execute(purge_zookeeper)
    execute(ensure_kafka_running)
    execute(remove_result_files)
    execute(create_topics, topics)

    execute(run_test, duration, message_size, topics)

    for host in h('prod'):
        current_log_dir = "{}/{}/{}/{}".format(local_log_directory, suite_name, set_name, host)
        local('mkdir -p {}'.format(current_log_dir))

        succeeded = False
        while(not succeeded):
            try:
                download_file_error(host, results_file_path, current_log_dir)
                succeeded = True
            except RemoteException:
                pass # succeed is already false

        download_file(host, kafka_log_file, current_log_dir)

    execute(log_test_set_execution_end, set_name)

@task
@runs_once
def run_test_suite(topics='[1]', series=1, duration=60.0, message_size=500, threads=3):
    suite_name = datetime.datetime.now().strftime('%d%m%y_%H%M')
    suite_log_dir = "{}/{}".format(local_log_directory, suite_name)
    local('mkdir -p {}'.format(suite_log_dir))

    try:
        execute(init)
        execute(ensure_zk_running)
        execute(restart_benchmark_daemons, threads)

        topic_progression = ast.literal_eval(topics)
        for i in range(0, len(topic_progression)):
            for j in range(0, int(series)):
                set_name = 't{}_{}'.format(str(topic_progression[i]), str(j))
                execute(run_test_set, suite_name, set_name, duration, message_size, topic_progression[i])

        for host in h('zk'):
            local_dir = '{}/{}/persuite/{}'.format(local_log_directory, suite_name, host)
            local('mkdir -p {}'.format(local_dir))
            download_file(host, zookeeper_log_file, local_dir)
            download_file(host, coordinator_log_path, local_dir)

        for host in h('prod'):
            local_dir = '{}/{}/persuite/{}'.format(local_log_directory, suite_name, host)
            local('mkdir -p {}'.format(local_dir))
            download_file(host, bench_service_log_path, local_dir)

        #analyze data
        suite_root = '{}/{}'.format(local_log_directory, suite_name)
        analysis_results_out_path = '{}/collective_results'.format(suite_root)
        local('python {} {} {} {} {} {} {} > {}'.format(analyzer_script_path,
                                                        suite_root,
                                                        topics,
                                                        series,
                                                        message_size,
                                                        duration,
                                                        len(env.roledefs['prod']),
                                                        analysis_results_out_path))
    finally:
        emergency_log_copy()



