# cloud-datascience-hadoop
Hadoop MapReduce to compare the yearly, monthly and seasonly comparisons of average temperature, wind and precipitation


##TECHNICAL SPECIFICATIONS
    Platform: MAC OSX Yosemite (32 bit)
    Language Used: Java
    AWS Products Used: S3, EC2 (Ubuntu Instance Created) Tools Used: MAC terminal, Eclipse IDE
    
    
##RESULTS
    Yearly Comparison of Average temperature, wind and precipitation (Time taken = 7699 milliseconds)
    Monthly Comparison of Average temperature, wind and precipitation (Time taken = 6143 milliseconds)
    Seasonly Comparison of Average temperature, wind and precipitation (Time taken = 6745 milliseconds)
    
##MACHINE LEARNING:
    R language was used for K-means clustering. The software package used was R Studio. The following commands were run in R     Studio to get the clusters (Cluster Size here = 2): 
     Clustering based on the averages of temperature, wind and precipitation
      Data_cluster<-read.csv(“/Users/sumanthpikkili/Desktop/Averages.csv“)
      out_cluster<-kmeans(Data_cluster[,2:4],2)              
      result<-data.frame(cbind(Data_cluster,out_cluster$cluster))
      plot(result$Year,result$avgtemp,col=result$out_cluster.cluster)

     Clustering based on the average of temperature
      Data_cluster<-read.csv(“/Users/sumanthpikkili/Desktop/Averages.csv“) out_cluster<-kmeans(Data_cluster$avgtemp,2)             result<-data.frame(cbind(Data_cluster,out_cluster$cluster)) 
      plot(result$Year,result$avgtemp,col=result$out_cluster.cluster)
