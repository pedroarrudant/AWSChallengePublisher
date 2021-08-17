using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AWSChallengePublisher
{
    public class Function
    {
        
        /// <summary>
        /// A simple function that takes a string and does a ToUpper
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(ILambdaContext context)
        {
            string bucketName = "stack-challenge-s3bucket-1ta8g5yqbeblp";
            var S3Client = new AmazonS3Client();
            GetObjectResponse response = new GetObjectResponse();
            List<Registro> listaRegistro = new List<Registro>();
            var SQSClient = new AmazonSQSClient();
            var queueURL = "https://sqs.us-east-1.amazonaws.com/099204064367/challenge-queue";

            ListObjectsV2Request requestList = new ListObjectsV2Request();
            requestList.BucketName = bucketName;

            ListObjectsV2Response responseList = await S3Client.ListObjectsV2Async(requestList);

            foreach (S3Object file in responseList.S3Objects)
            {
                var request = new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = file.Key
                };
                response = await S3Client.GetObjectAsync(request);
            }

            using (var reader = new StreamReader(response.ResponseStream))
            {
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(';');

                    var registro = new Registro
                    {
                        Indice = values[0],
                        Modelo = values[1],
                        Score = values[2],
                        Restritivo = values[3],
                        Positivo = values[4],
                        Mensagem = values[5],
                        AnoMesDia = values[6],
                    };

                    //listaRegistro.Add(registro);
                    await SQSClient.SendMessageAsync(queueURL, JsonConvert.SerializeObject(registro));
                }
            }
        }
    }
}
