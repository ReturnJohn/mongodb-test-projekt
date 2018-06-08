using System;
using System.Diagnostics.Contracts;
using System.Text;
using RabbitMQ.Client;

namespace Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            //instatiate the connect and channel  
            var connectionFactory = new ConnectionFactory();
            IConnection connection = connectionFactory.CreateConnection();
            IModel channel = connection.CreateModel();
            //for loop that adds a message to the queue 
            channel.QueueDeclare("Create", false, false, false, null);

            while (true)
            {

                for (int i = 0; i < 50000; i++)
                {
                    //creates a random int that then goes into a switch witch then chances the message to the coresponding command 
                    Random random = new Random();
                    int rndNr = random.Next(0, 6);
                    string command = "temp";
                    switch (rndNr)
                    {
                        case 0:
                            command = "create";

                            break;
                        case 1:

                            command = "read";

                            break;

                        case 2:
                            command = "update";

                            break;

                        case 3:
                            command = "delete";

                            break;
                        default:
                            command = "create";
                            break;

                    }

                    byte[] message = Encoding.UTF8.GetBytes(command);
                    //publishes the message to the declared queue 
                    channel.BasicPublish(string.Empty, "Create", null, message);
                }
                Console.WriteLine("will sleep");
                System.Threading.Thread.Sleep(250000);
            }


            Console.WriteLine("Press any key to exit");
            Console.ReadKey();
            channel.Close();
            connection.Close();
            Console.ReadKey();
        }
 
      
    }
}