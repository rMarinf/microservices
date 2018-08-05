import { NestFactory } from '@nestjs/core';
import { Transport } from '@nestjs/microservices';
import { ApplicationModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(ApplicationModule);

  // app.connectMicroservice({
  //   strategy: new RabbitMQServer('amqp://localhost:5672', 'channel'),
  // });

  // CÓDIGO DE CONEXION VIA REDIS
  app.connectMicroservice({
   transport: Transport.REDIS,
   options: {
     url: 'redis://localhost:6379',
   },
  });

  // CODIGO DE CONEXIÓN VIA TCP
  // app.connectMicroservice({
  //   transport: Transport.TCP,
  //   options: {
  //     retryAttempts: 5,
  //     retryDelay: 3000,
  //     port: 3000,
  //   },
  // });

  await app.startAllMicroservicesAsync();
  await app.listen(3002);
}
bootstrap();
