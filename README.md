# Asynchronous AMQP base Consumer and Producer

AMQP Consumer and Producer base classes. Working on top of `aioamqp`.

## Usage

Producer:
   
    from aioamqp_ext import BaseProducer
    
    producer = BaseProducer(**{
        'url': 'amqp://localhost:5672/',
        'exchange': 'my_exchange'
    })
    producer.publish_message(
        payload='foo',
        routing_key='bar'
    )
    producer.close()

Consumer:

    from aioamqp_ext import BaseConsumer
    
    class Consumer(BaseConsumer):
        async def process_request(self, data):
            print(data)
    
    if __name__ == "__main__":
        consumer = Consumer(**{
            'url': 'amqp://localhost:5672/',
            'exchange': 'my_exchange',
            'queue': 'my_queue',
            'routing_key': 'foo.bar'
        })
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(consumer.consume())
            loop.run_forever()
        except KeyboardInterrupt:
            loop.run_until_complete(consumer.close())
        finally:
            loop.close()
    
## Tests

To run the tests, you'll need to install the Python test dependencies::

    pip install -r ./requirements-ci.txt 

Then you can run the tests with `make unit-test `.
