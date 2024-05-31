package com.nextrow.data.converter.controller;

import com.mongodb.client.*;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.tomcat.util.json.JSONParser;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class CSV_Controller {

    MongoClient mongoClient= MongoClients.create();
    MongoDatabase mongoDatabase=mongoClient.getDatabase("data-converter");
    MongoCollection<Document> mongoCollection=mongoDatabase.getCollection("movies-list");


    JSONObject totalData=new JSONObject();

    @GetMapping("/convertDataToJson")
    public Map<String, Object> convertDataToJson() throws IOException, CsvValidationException {
        CSVReader csvReader=new CSVReader(new FileReader("movies.csv"));
        String[] header= csvReader.readNext();
        for(String s:header){
            System.out.println(s);
        }
        String[] values;
        while((values=csvReader.readNext())!=null){
            JSONObject data=new JSONObject();
            for(int i=0;i<header.length;i++)
            {
                data.put(header[i],values[i]);
            }
            totalData.put(values[4],data);
        }
        File jsonFile=new File("jsonFile.json");
        FileWriter fileWriter=new FileWriter(jsonFile);
        Document document=new Document(totalData.toMap());
        fileWriter.append(document.toJson());
        return totalData.toMap();
    }

    @GetMapping("/jsonToMongoDB")
    public void jsonToMongoDB(){

        LocalTime localTime=LocalTime.now();
        System.out.println(localTime);
        int i=0;
        Map<String,Object> data=totalData.toMap();
        for(String key: data.keySet()){
            HashMap<String,Object> hashMap=new HashMap<>();
            hashMap.put(key,data.get(key));
            Document document=new Document(hashMap);
            mongoCollection.insertOne(document);
            i++;
        }
        System.out.println("Done: "+i);
        LocalTime localTime1=LocalTime.now();
        System.out.println(localTime1);
    }

    @GetMapping("/mongoToJson")
    public Map<String, Object> mongoToJson() throws IOException {
        JSONObject jsonObject=new JSONObject();
        System.out.println(mongoCollection.countDocuments());
        HashMap<String,Object> hashMap=new HashMap<>();
        for(Document document:mongoCollection.find())
        {
            document.remove("_id");
            for(String string:document.keySet()){
                hashMap.put(string,document.get(string));
            }
        }
        jsonObject.put("Movie",hashMap);
        File jsonFile=new File("mongoToJson.json");
        FileWriter fileWriter=new FileWriter(jsonFile);
        Document document=new Document(jsonObject.toMap());
        fileWriter.append(document.toJson());

        return jsonObject.toMap();
    }

    @GetMapping("/mongoToCSV")
    public void mongoToCSV() throws IOException {

        LocalTime localTime=LocalTime.now();
        System.out.println(localTime);

        File file=new File("mongoToCSV.csv");
        CSVWriter csvWriter=new CSVWriter(new FileWriter(file));

        Document doc = mongoCollection.find().first();
        doc.remove("_id");

        JSONObject json = new JSONObject(doc.toJson());

        for (String k : json.keySet()) {

            JSONObject jsonObject1 = json.getJSONObject(k);
            JSONArray jsonArray = new JSONArray();
            ArrayList<String> array=new ArrayList<>();

            for (String ke : jsonObject1.keySet()) {
                array.add(ke);
                jsonArray.put(ke);
            }

            String[] header = array.toArray(new String[0]);
                    //jsonArray.toList().toArray(new String[0]);
            csvWriter.writeNext(header);
//            csvWriter.flush();
        }


        for(Document document:mongoCollection.find())
        {
            document.remove("_id");
            for (String s: document.keySet()){
                ArrayList<String > arrayList=new ArrayList<>();
                JSONObject jsonObject=new JSONObject(document.toJson());
                for(String k: jsonObject.keySet())
                {
                    JSONObject jsonObject1 = (JSONObject) jsonObject.get(k);
                    for(String ke: jsonObject1.keySet())
                    {
                        arrayList.add(jsonObject1.get(ke).toString());
                    }
                    String[] header=arrayList.toArray(new String[0]);
                    csvWriter.writeNext(header);

                }
            }
        }
//        csvWriter.flush();
        LocalTime localTime1=LocalTime.now();
        System.out.println(localTime1);
    }

}


/*        for(Document document:mongoCollection.find())
        {
            document.remove("_id");
            for (String s: document.keySet()){
                ArrayList<String > arrayList=new ArrayList<>();
                JSONObject jsonObject=new JSONObject(document.toJson());//title
                for(String k: jsonObject.keySet())
                {
                    JSONObject jsonObject1 = (JSONObject) jsonObject.get(k);// key and values
                    for(String ke: jsonObject1.keySet())
                    {
                        arrayList.add(ke);
                    }
                    String[] header=arrayList.toArray(new String[0]);
                    csvWriter.writeNext(header);
                    csvWriter.flush();
                }
            }
            break;
        }
*/