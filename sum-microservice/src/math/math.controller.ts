import { Controller, Get } from '@nestjs/common';
import {
  ClientProxy,
  MessagePattern,
} from '@nestjs/microservices';
import { ClientRMQ } from "common/custom-transports/rabbitmq.client";
import { Observable } from 'rxjs';

interface Mult {
  mult: number;
}

interface Divide {
  divide: number;
}
@Controller()
export class MathController {
  client: ClientProxy = new ClientRMQ({
    transport: 'RMQ',
    options: {
      url: 'amqp://localhost:5672',
      queue: 'test',
      queueOptions: { durable: false },
    },
  });

  // TODO : CODIGO PARA CONECTARLOS VIA REDIS
  // @Client({
  //   transport: Transport.REDIS,
  //   options: {
  //     url: 'redis://localhost:6379',
  //   },
  // })
  // client: ClientProxy;

  // TODO: CODIGO PARA CONECTARLOS VIA TCP
  // @Client({
  //   transport: Transport.TCP,
  //   options: {
  //     port: 3002,
  //   },
  // })
  // client: ClientProxy;

  @Get()
  callSum(): Observable<number> {
    const pattern = { cmd: 'sum' };
    const data = [1, 2, 3, 4, 5];
    return this.client.send<number>(pattern, data);
  }

  @Get('/sumAndDivide')
  callSumAndDivide(): Observable<number> {
    const pattern = { cmd: 'sumAndDivide' };
    const data = [1, 2, 3, 4, 5];
    return this.client.send<number>(pattern, data);
  }

  @Get('/send')
  async send(): Promise<object> {
    const pattern = { cmd: 'sumAndDivide' };
    const data = [1, 2, 3, 4, 5];
    // Call to sum microservice
    const result = await this.client.send<Divide>(pattern, data).toPromise();

    // Call to mult microservice
    // Mult X 2 this result
    const dataToMul = result.divide;
    const resultX2 = await this.client.send<number>({ cmd: 'mulX2' }, dataToMul).toPromise();
    console.log(resultX2, '<<<< result X 2');
    return { ok : true };
  }

  @MessagePattern({ cmd: 'mul' })
  mul(data: number[]): Mult {
    const totalSum = (data || []).reduce((a, b) => a * b);
    return { mult: totalSum };
  }

  @MessagePattern({ cmd: 'mulX2' })
  mulX2(data: number): Mult {
    return { mult: data * 2 };
  }
}
