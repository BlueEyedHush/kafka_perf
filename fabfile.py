# todo: add analyzer part
# todo: use fabric.operations.get
# todo: add host aliases
# todo: redirect command outputs to coordinator

import ast
import datetime
import os
from fabric.api import *

env.shell = "/bin/bash -c"
env.always_use_pty = False
env.use_ssh_config = True

client_jmx_port=9091
zk_jmx_port=9091
kafka_jmx_port=9093

jmx_options='-Dcom.sun.management.jmxremote ' \
            '-Dcom.sun.management.jmxremote.port={} ' \
            '-Dcom.sun.management.jmxremote.local.only=false ' \
            '-Dcom.sun.management.jmxremote.authenticate=false ' \
            '-Dcom.sun.management.jmxremote.ssl=false'.format(client_jmx_port)

class RemoteException(Exception):
    pass
env.abort_exception = RemoteException

jar_name = 'kafka_perf_test-0.2-jar-with-dependencies.jar'
kafka_heap = '-Xmx4G -Xms4G'

# paths on local machine
local_python_dir='./src/main/python'
orchestrator_script_path='{}/orchestrator.py'.format(local_python_dir)
analyzer_script_path = '{}/analyzer.py'.format(local_python_dir)
local_log_directory = './logs' # downloaded logs and results are stored there
emergency_local_log_directory = '{}/emergency'.format(local_log_directory) # in case of serious failure during
                                                                           # execution logs will be copied here
# paths on remote machines (all)
remote_log_directory = '~/log/kafka_perf'
coordinator_log_path = '{}/coordinator.out'.format(remote_log_directory) # this file is stored remotelly,
                                                                    # and then copied somewhere under local log dir
bench_service_log_path = '{}/bench.out'.format(remote_log_directory)
zookeeper_log_file = '{}/zookeeper.out'.format(remote_log_directory)

results_file_path = '/tmp/results'

# paths on remote machines (itrac)
bundle_dir = '/data1/cals/kafka_perf/bundle'
kf_bench_dir = '/data1/cals/kafka_perf/bench'

kafka_dir = '{}/kafka/latest'.format(bundle_dir)
kafka_topic_creation_script_path = '{}/bin/kafka-topics.sh'.format(kafka_dir)
zookeeper_dir = '{}/zookeeper/latest'.format(bundle_dir)

kafka_d_mount_point_prefix = '/data'
kafka_d_first_mount_number = 1
kafka_d_last_mount_number = 8
kafka_data_dir_suffix = '/cals/kafka_perf/data/kf'

kafka_log_file = '{}/logs/kafkaServer.out'.format(kafka_dir)

# paths on remote machines (openstack)
bench_dir = '/opt/kafka_perf/bench'

python_sources_dir = '{}/src/main/python'.format(bench_dir)
test_worker_jar = '{}/target/{}'.format(bench_dir, jar_name)

# aliases
a = {
    'i9': 'itrac1509.cern.ch',
    'i10': 'itrac1510.cern.ch',
    'i11': 'itrac1511.cern.ch',
    'i12': 'itrac1512.cern.ch',
    'o1': 'cals-kafka-perf-bf4cbe5f-709f-4158-b1de-f49e3d0dfeef.cern.ch',
    'o2': 'cals-kafka-perf-df4766f8-d34a-406c-b751-8372efcdde22.cern.ch',
    'o3': 'cals-kafka-perf-9eb4ca80-8369-496f-b455-9c25d6ad4a8b.cern.ch',
    'o4': 'cals-kafka-perf-bbcc82bc-1943-4297-bb48-d6b536ad83e4.cern.ch',
    'o5': 'cals-kafka-perf-f61d20da-c57c-4536-87f5-330b2ded8b74.cern.ch',
    'o6': 'cals-kafka-perf-a6fd3630-4503-474a-a11c-f968f6d70f03.cern.ch',
    'o7': 'cals-kafka-perf-4dc7e768-ccce-4f6e-9c6f-eccc6082ee2c.cern.ch',
    'o8': 'cals-kafka-perf-0903fdc7-1269-4e9f-8edc-1f526b42da04.cern.ch',
}

# groups of hosts
env.roledefs = {
    'all': a.values(),
    'kafka': [a['i9'], a['i10'], a['i11']],
    'zk': [a['i11']],
    'prod': [a['o1'], a['o2'], a['o3'], a['o4'], a['o5'], a['o6'], a['o7'], a['o8']],
    'zk_operator': [a['o8']], # node from which all commands to zk will be issued
    'zk_chosen': [a['i11']]
}

def coord_log(msg):
    #run('echo "[`date`]: {0}" >> {1}'.format(msg, coordinator_log_path))
    current_time = datetime.datetime.now()
    print '[{}] {}'.format(str(current_time), msg)


def download_file_error(host, from_path, to_path):
    os.system('scp {}:{} {}'.format(host, from_path, to_path))

def download_file(host, from_path, to_path):
    os.system('scp {}:{} {} || true'.format(host, from_path, to_path))

def get_and_ensure_existence_of_emergency_logdir_for(host):
    host_log_dir = '{}/{}'.format(emergency_local_log_directory, host)
    os.system('mkdir -p {}'.format(host_log_dir))
    return host_log_dir

def emergency_log_copy():
    for host in h('all'):
        host_log_dir = get_and_ensure_existence_of_emergency_logdir_for(host)
        download_file(host, coordinator_log_path, host_log_dir)

    # for host in h('zk'):
    #     host_log_dir = get_and_ensure_existence_of_emergency_logdir_for(host)
    #     download_file(host, zookeeper_log_file, host_log_dir)

    for host in h('kafka'):
        host_log_dir = get_and_ensure_existence_of_emergency_logdir_for(host)
        download_file(host, kafka_log_file, host_log_dir)

    for host in h('prod'):
        host_log_dir = get_and_ensure_existence_of_emergency_logdir_for(host)
        download_file(host, bench_service_log_path, host_log_dir)

def h(name):
    return env.roledefs[name]

def run_daemonized(cmd, log_path):
    run('nohup {0} > {1} 2>&1 < /dev/null &'.format(cmd, log_path))

def ignore_err(cmd):
    return '{} || true'.format(cmd)

@task
@roles('all')
def init():
    run(ignore_err('mkdir -p {}'.format(remote_log_directory)))
    run('echo ---------`date`--------- > {}'.format(coordinator_log_path))
    local(ignore_err('mkdir -p {}'.format(local_log_directory)))
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
@roles('prod')
def restart_benchmark_daemons(threads, sid, throttle_at):
    coord_log('stopping testing daemons')
    run('''for pid in `ps aux | grep [k]afka_perf_test | awk '{print $2}' | tr '\n' ' '`; do kill -s 9 $pid; done''')
    run(ignore_err('mv {} /tmp'.format(bench_service_log_path)))
    coord_log('starting testing daemons')

    curr_no = env.all_hosts.index(env.host_string)
    all = len(env.all_hosts)
    id = '{},{}'.format(curr_no, all)
    run_daemonized('java {} -jar {} -t {} -s {} -T {} -p {}'.format(jmx_options, test_worker_jar, threads, sid, throttle_at, id), bench_service_log_path)

@task
@parallel
@roles('zk')
def ensure_zk_running():
    zk_server_script_path = '{}/bin/zkServer.sh'.format(zookeeper_dir)
    coord_log('ensuring that zk is running')
    # coord_log('ensuring that zk is running & truncating log')
    # run('echo ----trunc---- > {}'.format(zookeeper_log_file))
    run('export ZOO_LOG_DIR={0} && export JMXPORT={1} && {2} start'
                     .format(remote_log_directory, zk_jmx_port, zk_server_script_path))
    coord_log('zk should be running')

@task
@roles('zk_operator')
def purge_zookeeper():
    purge_script_path = '{0}/zkDelAll.py'.format(python_sources_dir)
    coord_log('removing all znodes except /zookeeper')
    run('python -u {0} /'''.format(purge_script_path))
    coord_log('purging complete')

def foreach_mount_point(cmd):
    run('for num in $(seq {} {}); do {} {}${{num}}{} || true; done'.format(
            kafka_d_first_mount_number,
            kafka_d_last_mount_number,
            cmd,
            kafka_d_mount_point_prefix,
            kafka_data_dir_suffix))

@task
@parallel
@roles('kafka')
def cleanup_after_kafka():
    coord_log('emptying kafka log directories')
    foreach_mount_point('rm -rf')
    foreach_mount_point('mkdir -p')
    coord_log('kafka log directories emptied')

@task
@parallel
@roles('kafka')
def remove_kafka_log():
    run(ignore_err('rm -f {}'.format(kafka_log_file)))

@task
@parallel
@roles('prod')
def remove_result_files():
    run(ignore_err('rm -rf {}'.format(results_file_path)))

@task
@parallel
@roles('kafka')
def ensure_kafka_running():
    kafka_start_script_path = '{0}/bin/kafka-server-start.sh'.format(kafka_dir)
    kafka_config_path = '{0}/config/server.properties'.format(kafka_dir)
    coord_log('ensuring kafka is running')
    run('export JMX_PORT={} && export KAFKA_HEAP_OPTS="{}" && {} -daemon {}'
                     .format(kafka_jmx_port, kafka_heap, kafka_start_script_path, kafka_config_path))
    coord_log('kafka should be running')

@task
@roles('zk_chosen')
def create_topic(partitions):
    coord_log('creating topic')
    run('{} --zookeeper localhost:2181 --create --topic 0 --partitions {} --replication-factor 3'
                     .format(kafka_topic_creation_script_path, partitions))
    coord_log('topic created')

@task
@parallel
@roles('prod')
def log_actual_testing_started():
    coord_log('actual testing started')

def run_test(duration, message_size, topics, partitions):
    execute(log_actual_testing_started)
    local('python -u {} -d {} -s {} -t {} -p {}'.format(orchestrator_script_path, duration, message_size, topics, partitions))
    local('sleep 5s')

@task
@parallel
@roles('all')
def log_test_set_execution_start(set_name, duration, message_size, partitions):
    coord_log('starting test set {} (duration = {}, message_size = {}, parititons = {})'
              .format(set_name, duration, message_size, partitions))

@task
@parallel
@roles('all')
def log_test_set_execution_end(set_name):
    coord_log('test set {} finished'.format(set_name))

@task
def restart_brokers(partitions):
    execute(stop_kafka)
    execute(cleanup_after_kafka)
    execute(remove_kafka_log)
    execute(purge_zookeeper)
    execute(ensure_kafka_running)
    local('sleep 10') # give kafka cluster some time to initialize
    execute(create_topic, partitions)

@task
def run_test_set(suite_log_dir, set_name, duration, message_size, topics, partitions, as_is):
    execute(log_test_set_execution_start, set_name, duration, message_size, partitions)

    if(not as_is):
        restart_brokers(partitions)

    execute(remove_result_files)
    execute(run_test, duration, message_size, topics, partitions)

    for host in h('prod'):
        current_log_dir = "{}/{}/{}".format(suite_log_dir, set_name, host)
        local('mkdir -p {}'.format(current_log_dir))

        succeeded = False
        while(not succeeded):
            try:
                download_file_error(host, results_file_path, current_log_dir)
                succeeded = True
            except RemoteException:
                pass # succeed is already false
    #
    # for host in h('kafka'):
    #     current_log_dir = "{}/{}/{}".format(suite_log_dir, set_name, host)
    #     local('mkdir -p {}'.format(current_log_dir))
    #
    #     download_file(host, kafka_log_file, current_log_dir)

    execute(log_test_set_execution_end, set_name)

def get_and_ensure_existence_of_persuite_log_dir_for(suite_log_dir, host):
    dir = '{}/persuite/{}'.format(suite_log_dir, host)
    local('mkdir -p {}'.format(dir))
    return dir


@task
@runs_once
def run_test_suite(suite_log_dir=None,
                   topics=1,
                   partitions='[1]',
                   series=1,
                   duration=60.0,
                   message_size=500,
                   threads=3,
                   sid="mtfl",
                   as_is=False,
                   throttle_at_throughput=None,
                   leave_daemons=False):
    suite_log_dir = suite_log_dir if suite_log_dir is not None \
        else "{}/{}".format(local_log_directory, datetime.datetime.now().strftime('%d%m%y_%H%M'))
    local('mkdir -p {}'.format(suite_log_dir))

    if throttle_at_throughput is None:
        throttle_at_messages = '-1'
    else:
        throttle_at_messages = int(float(throttle_at_throughput)/(int(message_size)*int(threads)*len(h('prod'))))

    try:
        execute(init)
        execute(ensure_zk_running)
        if(not leave_daemons):
            execute(restart_benchmark_daemons, threads, sid, throttle_at_messages)
            local('sleep 5s')

        partitions_parsed = ast.literal_eval(partitions)
        for p in partitions_parsed:
            for j in range(0, int(series)):
                set_name = 't{}_{}'.format(str(p), str(j))
                execute(run_test_set, suite_log_dir, set_name, duration, message_size, topics, p, as_is)

        # for host in h('zk'):
        #     local_dir = get_and_ensure_existence_of_persuite_log_dir_for(suite_log_dir, host)
        #     download_file(host, zookeeper_log_file, local_dir)

        # for host in h('all'):
        #     local_dir = get_and_ensure_existence_of_persuite_log_dir_for(suite_log_dir, host)
        #     download_file(host, coordinator_log_path, local_dir)
        #
        # for host in h('prod'):
        #     local_dir = get_and_ensure_existence_of_persuite_log_dir_for(suite_log_dir, host)
        #     download_file(host, bench_service_log_path, local_dir)

        #analyze data
        analysis_results_out_path = '{}/collective_results'.format(suite_log_dir)
        local('python {} {} {} {} {} {} {} > {}'.format(analyzer_script_path,
                                                        suite_log_dir,
                                                        partitions,
                                                        series,
                                                        message_size,
                                                        duration,
                                                        len(env.roledefs['prod']),
                                                        analysis_results_out_path))
    finally:
        pass
        # emergency_log_copy()
