# coding=utf-8

"""
删除Rabbitmq队列中的所有的消息
"""

import time
import pika
from threading import Thread
from django.http import JsonResponse

# 以下是rabbitmq的配置信息
RABBITMQ_HOST = '...'
RABBITMQ_PORT = '...'
RABBITMQ_USER = '...'
RABBITMQ_PASSWORD = '...'
RABBITMQ_VHOST = '...'  # 如果为 '/' 需要调整为 '%2F'


class MQBase:
    """
    消息队列基类
    """

    def __init__(self, host, port, username, password, vhost, exchange, exchange_type, ack=True, persist=True):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.ack = ack
        self.persist = persist
        self.param = pika.URLParameters(url="amqp://{0}:{1}@{2}:{3}/{4}".format(
            self.username, self.password, self.host, self.port, self.vhost
        ))
        
        self.conn = None
        self.channel = None

    def _open_channel(self):
        """
        建立连接并开辟信道、交换器信息
        """
        self.conn = pika.BlockingConnection(self.param)
        self.channel = self.conn.channel()
        self.channel.exchange_declare(exchange=self.exchange,
                                      exchange_type=self.exchange_type,
                                      durable=self.persist)
        
        if self.ack:
            self.channel.confirm_delivery()
    
    def _close_channel(self):
        """
        关闭信道并断开连接
        """
        if self.channel and self.channel.is_open:
            self.channel.close()

        if self.conn and self.conn.is_open:
            self.conn.close()


class MQReceiver(MQBase):
    """
    消息队列 - 消费者
    """

    def __init__(self, host, port, username, password, vhost, exchange, exchange_type, ack=True, persist=True, max_num=0):
        MQBase.__init__(self, host, port, username, password, vhost, exchange, exchange_type, ack=True, persist=True)
        self.max_num = max_num
        self.message_opt_num = 0
        self.message_loop_num = 0
        self._result = None

    def _declare_queue(self, queue_name):
        """
        声明队列
        """
        self.channel.queue_declare(queue=queue_name,
                                   durable=self.persist)
        self.channel.queue_bind(queue=queue_name,
                                exchange=self.exchange,
                                routing_key=queue_name)
    
    def _subscribe_queue(self, queue_name):
        """
        订阅队列
        """
        self._declare_queue(queue_name)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(consumer_callback=self.handler,
                                   queue=queue_name,
                                   no_ack=not self.ack)

    def handler(self, channel, method_frame, header_frame, body):
        """
        收到消息后的回调，即消息处理器
        """
        self.func(body)

        if self.ack:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        self.message_opt_num += 1
        if self.max_num != 0 and self.max_num <= self.message_opt_num:
            self._result = {
                'result': True,
                'message': "You have consumed {0} messages in celery queue.".format(self.message_opt_num)
            }
            self.end()

    def start(self, queue, func):
        """
        开始消费，用回调方式
        """
        self.func = func
        self._open_channel()
        self._subscribe_queue(queue)

        # 为了能够控制消费，这里没有使用 channel.start_consuming() 而是使用了 process_data_events
        while self.channel._consumer_infos:
            self.message_loop_num += 1
            self.channel.connection.process_data_events(time_limit=1)
            
            if (self.message_loop_num - self.message_opt_num) > 5:
                self._result = {
                    'result': True,
                    'message': "You have consumed {0} messages in celery queue, and there are no more messages in celery queue.".format(self.message_opt_num)
                }
                self.end()

    def end(self):
        """
        结束消费
        """
        self.channel.stop_consuming()
        self._close_channel()

    def get_result(self):
        """
        获取队列执行结果
        """
        return self._result


class CustomTask:
    """
    自定义任务
    """

    def __init__(self):
        self._result = None

    @staticmethod
    def opt_msg(msg):
        """
        消息处理器
        """
        # print("body: {0}".format(msg))
        pass

    def run(self, *args, **kwargs):
        receiver = MQReceiver(host=RABBITMQ_HOST,
                          port=RABBITMQ_PORT,
                          username=RABBITMQ_USER,
                          password=RABBITMQ_PASSWORD,
                          vhost=RABBITMQ_VHOST,
                          exchange='celery',
                          exchange_type='direct',
                          ack=True,
                          persist=True,
                          max_num=kwargs.get('max_num', 0))
        receiver.start('celery', self.opt_msg)
        receiver.end()
        self._result = receiver.get_result()

    def get_result(self):
        """
        返回自定义任务的执行结果
        """
        return self._result


def clear_celery_queue(request):
    """
    清理celery队列消息的HTTP请求
    """
    if not request.user.is_superuser:
        result = {
            'result': False,
            'message': 'request user auth denied.'
        }
        return JsonResponse(result)

    max_num = request.GET.get('max_num', 0)
    ct = CustomTask()

    th = Thread(target=ct.run, kwargs={'max_num': max_num})
    th.start()
    th.join()

    result = ct.get_result()
    return JsonResponse(result)


if __name__ == "__main__":
    print("rabbitmq consuming message ...")

    max_num = 3
    ct = CustomTask()

    th = Thread(target=ct.run, kwargs={'max_num': max_num})
    th.start()
    th.join()

    result = ct.get_result()

    print("rabbitmq consume end.\nresult={0}".format(result))



