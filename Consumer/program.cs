
using System;
using System.Text;
using System.Diagnostics;
using RabbitMQ.Client;

using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Generic;
using System.Linq;


namespace Consumer
{
    class Program
    {


      


        public static string[] firstName;
        public static string[] middleName;
        public static string[] lastName;

        public void fillNameArrays()
        {
            // instatiate the name arrays and fills them with 10 names each 
           
            firstName = new string[] { "hans", "nicklas", "mikkel", "nikolai", "wong", "karl" ,"wang", "ole", "per", "thomas"};

            middleName = new string[] { "mar", "nicklas", "mikkel", "ole", "wong", "karl", "wang", "ole", "per", "ib" };

            lastName = new string[] { "marsen", "jensen", "mikkelsen", "persen", "wongsen", "karlsen", "wangsen", "olesen", "sørensen", "ibsen" };

        }


        //variables for usage of the database 
        protected static IMongoClient _client;
        protected static IMongoDatabase _database;
        static List<long> createTimes = new List<long>();
        static List<long> deleteTimes = new List<long>();
        static List<long> updateTimes = new List<long>();
        static List<long> readTimes = new List<long>();
     
        public static Random r = new Random();
        public static int rndInt;
        public static Names GetName()
        {

            //create a new name with random first middle and last name, 
            
            Names name = new Names()
            {
                
                FirstName = firstName[rndInt],

                MiddleName = middleName[rndInt],

                LastName = lastName[rndInt],
            };

            return name;
        }
        


        public string findMaxValue( List<long> times)
        {
            string maxValue;
            long max = times.Max();
            
            maxValue = max.ToString();
            return maxValue;
        }
        public string findMinValue(List<long> times)
        {
            string minValue;
            long min = times.Min();

            minValue = min.ToString();
            return minValue;
        }
        public string findAverage(List<long> times)
        {
            string avr = "";
            if (times.Count > 1)
            {
                double average = times.Average();

                 avr =  average.ToString();
                
            }

            return avr;
        }
        
        public class Names
        {
            public ObjectId Id { get; set; }
            public string FirstName { get; set; }
            public string MiddleName { get; set; }
            public string LastName { get; set; }
            public long autonum { get; set; }
        }


        
        public static int cnt = 0;
        public void CRUD(string command, IMongoCollection<Names>  _collection)
        {
          
            if(command == "update")
            {

                //creates the name to find and replace
                var GammeltNavn = new Names();
                GammeltNavn.autonum = r.Next(cnt);
            
                var Nytnavn = new Names();
                Nytnavn.FirstName = "lars";
                //finds the name to replace and updates it to the new name 
                _collection.FindOneAndUpdate<Names>
                    (Builders<Names>.Filter.Eq("autonum", GammeltNavn.autonum),
                        Builders<Names>.Update.Set("FirstName", Nytnavn.FirstName));

            }

            if(command == "delete")
            {
                //Finds and Deletes the name with the correct value in autonum   

                _collection.DeleteOne(s => s.autonum == r.Next(cnt));
  
            }
        
            if(command == "create")
            {
                cnt++;
                //inserts a new name to the database 
                var n = GetName();
                n.autonum =  cnt;
                _collection.InsertOne(n);

            }

            if(command == "read")
            {
               
                //creates a filter that specifies a collum and the value it should be 
                
                var nameIsOle = new BsonDocument("autonum", r.Next(cnt));
                //find the first object that meets the criteria of the filter 
                _collection.Find<Names>(nameIsOle);

            } 
        }

        static void Main(string[] args)
        {
            Program p = new Program();
            //fills the name arrays with names
            p.fillNameArrays();
            
            rndInt = r.Next(firstName.Count<string>());
            //creates the connection channel and queue
            var connectionFactory = new ConnectionFactory();
            IConnection connection = connectionFactory.CreateConnection();
            IModel channel = connection.CreateModel();
            channel.QueueDeclare("Create", false, false, false, null);

           
            //intialize connection to the database 
            _client = new MongoClient();
            _database = _client.GetDatabase("nameDB");
            var _collection = _database.GetCollection<Names>("nameBase");


            while (true)
            {
                //clears the time lists 
                createTimes.Clear();
                readTimes.Clear();
                updateTimes.Clear();
                deleteTimes.Clear();

            //for loop for getting 5k messages from the queue
            for (int i = 0; i < 5000; i++)
            {
                BasicGetResult result = channel.BasicGet("Create", true);
                //checks if the message is null 
                if (result != null)
                {
                    Stopwatch stopwatch = new Stopwatch();
                    //converts the message and gives it to the CRUD funtion together with the database collection.
                    string message = Encoding.UTF8.GetString(result.Body);
                    stopwatch.Start();
                        //runs the crud funktions on the database 
                    p.CRUD(message, _collection);
                    stopwatch.Stop();
                        //checks what list the time should be added to and adds it to the list
                        if(message == "create")
                        {
                            createTimes.Add(stopwatch.ElapsedTicks);
                        }
                        if (message == "update")
                        {
                            updateTimes.Add(stopwatch.ElapsedTicks);
                        }
                        if (message == "delete")
                        {
                            deleteTimes.Add(stopwatch.ElapsedTicks);
                        }
                        if (message == "read")
                        {
                            readTimes.Add(stopwatch.ElapsedTicks);
                        }

                }

            }
            

            
            //write the avr max and min times from the different time lists 
                Console.WriteLine("avr READ  {0}:ticks.",p.findAverage(readTimes));
                Console.WriteLine("max READ  {0}:ticks",p.findMaxValue(readTimes));
                Console.WriteLine("min READ  {0}:ticks",p.findMinValue(readTimes));
                Console.WriteLine("");

                Console.WriteLine("avr DELETE  {0}:ticks", p.findAverage(deleteTimes));
                Console.WriteLine("max DELETE  {0}:ticks", p.findMaxValue(deleteTimes));
                Console.WriteLine("min DELETE  {0}:ticks", p.findMinValue(deleteTimes));
                Console.WriteLine("");

                Console.WriteLine("avr UPDATE  {0}:ticks", p.findAverage(updateTimes));
                Console.WriteLine("max UPDATE  {0}:ticks", p.findMaxValue(updateTimes));
                Console.WriteLine("min UPDATE  {0}:ticks", p.findMinValue(updateTimes));
                Console.WriteLine("");

                Console.WriteLine("avr CREATE  {0}:ticks", p.findAverage(createTimes));
                Console.WriteLine("max CREATE  {0}:ticks", p.findMaxValue(createTimes));
                Console.WriteLine("min CREATE  {0}:ticks", p.findMinValue(createTimes));
                Console.WriteLine("");
            }

            Console.WriteLine("Press any key to exit");
            Console.ReadKey();
            //closes the connection and channel
            channel.Close();
            connection.Close();
            Console.ReadKey();
        }
      
    }
}