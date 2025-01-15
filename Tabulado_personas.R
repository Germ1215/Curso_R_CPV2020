
library(sparklyr)
library(dplyr)
library(tidyverse)
library(lubridate)
library(stringr)
library(knitr)
library(openxlsx)
library(scales) # Para poner comas y porcentajes
library(DBI)
library(rJava)
library(RJDBC)
library(tidyverse)
library(ggplot2)
library(knitr)
library(kableExtra)

Sys.setenv(JAVA_HOME="C:/Program Files/Java/jre1.8.0_431")
config <- spark_config()
config[["sparklyr.jars.default"]] <- "C:/drivers/oracle/ojdbc8.jar"
config$`sparklyr.shell.driver-memory` <- "10G"
config$spark.memory.fraction <- 0.8
sc<-spark_connect(master="local", config=config,version="3.5.3")

#######################################
# PERSONAS
#######################################

# Lectura de archivo .CSV
#setwd("C:/Users/CITLALI.MUNOZ/Documents/2024/10 Oct/Proy_Boletin/Proy_Boletin_Autom")
archivo <- "D:/Scripts/R_git/Tabulados_censo2020/Personas01.csv"

#--------------------------------------------------------------------------------
#Leer el archivo .cv al ambiente Spark
#--------------------------------------------------------------------------------
t <- proc.time()
spark_read_csv(sc,
               name = "db",
               path = archivo,
               delimiter = ",",
               header = TRUE)
proc.time() - t

db<-tbl(sc, "db") #%>% sdf_sample(fraction = 0.80, replace = FALSE,seed=123)#Función "tbl" para copiar tabla spark a local R


#--------------------------------------------------------------------------------
#Visualización de las variables
#--------------------------------------------------------------------------------
n <- db %>% sdf_nrow() 
colnames(db)


#--------------------------------------------------------------------------------
#Cálculo de población con el factor de expansión
#--------------------------------------------------------------------------------
head(db) 
tab_sexo<-db %>%
  group_by(MUN,SEXO) %>% 
  mutate(N=sum(FACTOR)) %>% 
  select(MUN,SEXO,N) %>%
  collect() %>% 
  unique()


#--------------------------------------------------------------------------------
# Transponer columna (SEXO) en forma de fila
#--------------------------------------------------------------------------------
library(reshape2)
df_melt <- melt(tab_sexo, measure.vars = "N")
Total_sexo<-dcast(df_melt, MUN ~ SEXO + variable, value.var = "value")
Total_sexo<-data.frame(Total_sexo)
Total_sexo$TOTAL <-rowSums(Total_sexo[,c(-1)])

#--------------------------------------------------------------------------------
#Cálculo de la relación hombre/mujeres
#--------------------------------------------------------------------------------
Total_sexo$RELACION<-(Total_sexo[,"X1_N"]/Total_sexo[,"X3_N"]*100)
Total_sexo$RELACION<-round(Total_sexo$RELACION,2)

#--------------------------------------------------------------------------------
#Crear etiqueta de municipios
#--------------------------------------------------------------------------------
Total_sexo <- data.frame(Total_sexo) %>% mutate(Label_MUN = case_when(
  MUN == "1" ~ "Aguascalientes",
  MUN == "2" ~ "Asientos",
  MUN == "3" ~ "Calvillo",
  MUN == "4" ~ "Cosío",
  MUN == "5" ~ "Jesús María",
  MUN == "6" ~ "Pabellón de Arteaga",
  MUN == "7" ~ "Rincón de Romos",
  MUN == "8" ~ "San José de Gracia",
  MUN == "9" ~ "Tepezalá",
  MUN == "10" ~ "El Llano",
  MUN == "11" ~ "San Francisco de los Romo"
))

#--------------------------------------------------------------------------------
#Ordenar y etiquetar columnas
#--------------------------------------------------------------------------------
Total_sexo<-Total_sexo[,c(1,6,4,2,3,5)]
colnames(Total_sexo)<-c("MUN","Entidad federativa","Población total","Hombres","Mujeres","Relación hombres-mujeres")

library(openxlsx2)
library(openxlsx)


#--------------------------------------------------------------------------------
# Exportar las tablas con el paquete "xlsx" y "openxlsx"
#--------------------------------------------------------------------------------

write.xlsx(Total_sexo[,c(-1)],"Personas.xlsx")

worksheet_export1(Total_sexo[,c(-1)],
                  "Personas_formato.xlsx",
                  "Relacio_Hombre_Mujeres",
                  "#3DAE2B")



#######################################
# VIVIENDAS
#######################################

#--------------------------------------------------------------------------------
#Leer el archivo .cv al ambiente Spark
#--------------------------------------------------------------------------------
archivo2 <- "D:/Scripts/R_git/Tabulados_censo2020/Viviendas01.csv"
spark_read_csv(sc,
               name = "db2",
               path = archivo2,
               delimiter = ",",
               header = TRUE)

db2<-tbl(sc, "db2") #%>% sdf_sample(fraction = 0.80, replace = FALSE,seed=123)#Función "tbl" para copiar tabla spark a local R

#--------------------------------------------------------------------------------
#Visualización de las variables
#--------------------------------------------------------------------------------
n <- db2 %>% sdf_nrow() 
colnames(db2)
db2 %>%select(CLAVIVP) %>% collect() %>% unique()

#--------------------------------------------------------------------------------
#Cálculo de las viviendas con el factor de expansión
#--------------------------------------------------------------------------------
  tab_piso<-db2 %>%
    filter(!is.na(PISOS)) %>% 
    group_by(MUN,PISOS) %>% 
    mutate(N=sum(FACTOR)) %>% 
    select(MUN,PISOS,N) %>%
    collect() %>% 
    unique()

#--------------------------------------------------------------------------------
# Transponer columna (PISOS) en forma de fila
#--------------------------------------------------------------------------------
  df_melt2 <- melt(tab_piso, measure.vars = "N")
  Total_piso<-dcast(df_melt2, MUN ~ PISOS + variable, value.var = "value")
  Total_piso<-data.frame(Total_piso)
  
  #--------------------------------------------------------------------------------
  #Cálculo de los totales de viviendas(calculo por hileras)
  #--------------------------------------------------------------------------------
  Total_piso$TOTAL <-rowSums(Total_piso[,c(-1)],na.rm=T)

  #--------------------------------------------------------------------------------
  #Cálculo de los totales de viviendas(calculo por columnas)
  #--------------------------------------------------------------------------------
  tab_nacional<-data.frame(t(data.frame(colSums(Total_piso[,c(-1)],na.rm=T))))
  tab_nacional$MUN<-0
  tab_nacional$Label_MUN<-"Nacional"

  #--------------------------------------------------------------------------------
  #Crear etiqueta de municipios
  #--------------------------------------------------------------------------------
  Total_piso <- data.frame(Total_piso) %>% mutate(Label_MUN = case_when(
    MUN == "1" ~ "Aguascalientes",
    MUN == "2" ~ "Asientos",
    MUN == "3" ~ "Calvillo",
    MUN == "4" ~ "Cosío",
    MUN == "5" ~ "Jesús María",
    MUN == "6" ~ "Pabellón de Arteaga",
    MUN == "7" ~ "Rincón de Romos",
    MUN == "8" ~ "San José de Gracia",
    MUN == "9" ~ "Tepezalá",
    MUN == "10" ~ "El Llano",
    MUN == "11" ~ "San Francisco de los Romo"
  ))
  
  Total_piso<-rbind(Total_piso,tab_nacional)
  Total_piso<-data.frame(Total_piso[order(Total_piso$MUN),])
  
  #--------------------------------------------------------------------------------
  #Ordenar y etiquetar columnas
  #--------------------------------------------------------------------------------
  Total_piso<-Total_piso[,c(1,7,6,2,3,4,5)]
  colnames(Total_piso)<-c("MUN","Entidad federativa","Viviendas particulares",
                          "Tierra","Cemento o firme","Madera,mosaico u otro","No especificado")

  #--------------------------------------------------------------------------------
  # Exportar las tablas con el paquete "xlsx" y "openxlsx"
  #--------------------------------------------------------------------------------
  
write.xlsx(Total_piso[,c(-1)],"Viviendas_piso.xlsx")

worksheet_export1(Total_piso[,c(-1)],
                  "Viviendas_piso_formato.xlsx",
                  "Vivienda",
                  "#3DAE2B")


#####################################################
#FUNCIONES PARA EXPORTAR ARCHIVO CON FORMATOS
#####################################################

worksheet_export1<-function(df,file_xlsx,sheets,col_colum){
  
  df_list <- list()
  df_list[[1]] <- df
  
  width_col<-as.data.frame(t(data.frame(lapply(df, function(x) max(nchar(x))))))
  colnames(width_col)<-"ncaracter"
  width_col$ncaracter<-as.numeric(width_col$ncaracter)
  
  width_col<-width_col %>%
    mutate(ancho = case_when(ncaracter<=10 ~ 15,
                             ncaracter>10 & ncaracter<50 ~ 25,
                             ncaracter>=50 & ncaracter<100 ~ 50,
                             ncaracter>=100 ~ 75,
                             TRUE ~ 15)
    )
  
  wb <- createWorkbook()
  
  for (k in 1:length(sheets)){
    
    addWorksheet(wb, sheet = sheets[k])
    writeDataTable(wb,
                   sheets[k],
                   data.frame(df_list[k])
                   #,tableStyle = Style
    )
    
    #modifyBaseFont(wb, fontSize = 11, fontName = "Arial")
    #setColWidths(wb,sheets[k],cols = 1:ncol(data.frame(df_list[k])),widths = "auto")
    setColWidths(wb,sheets[k],cols = 1:ncol(data.frame(df_list[k])),widths = as.vector(width_col$ancho))#Ancho de columnas
    setRowHeights(wb, sheets[k],rows = 2:nrow(data.frame(df_list[k])),heights = 30)#Ancho de las hileras
    
    headerStyle <- createStyle(fontSize=11,fontColour="white",fontName = "Arial",fgFill=col_colum,
                               halign="center",valign="center",textDecoration="italic",
                               borderColour = "black",border = "TopBottom",borderStyle = "double")#Estilo de encabezado
    
    centerWrapStyle <- createStyle(halign = "center", valign = "center", wrapText = TRUE)#Ajuste de texto, centro
    leftAlignStyle <- createStyle(halign = "left", valign = "center",wrapText = TRUE)#Ajuste de texto, izquierdo
    rightAlignStyle <- createStyle(halign = "right", valign = "center",wrapText = TRUE)#Ajuste de texto, derecho
    border <- createStyle(border = "TopBottomLeftRight", borderStyle = "dashed")# Estilos de bordes
    grayStyle <- createStyle(fgFill = "#D3D3D3",halign = "left", valign = "center",wrapText = TRUE,border = "TopBottomLeftRight", borderStyle = "dashed")  # Light gray
    whiteStyle <- createStyle(fgFill = "#FFFFFF",halign = "left", valign = "center",wrapText = TRUE,border = "TopBottomLeftRight", borderStyle = "dashed")  # White
    
    #Aplicar todos los estilos
    addStyle(wb, sheet = sheets[k], style = headerStyle, rows = 1, cols = 1:ncol(data.frame(df_list[k])), gridExpand = TRUE)
    addStyle(wb, sheet = sheets[k], style = centerWrapStyle, rows = 2:nrow(data.frame(df_list[k])),cols = 1:ncol(data.frame(df_list[k])), gridExpand = TRUE)
    addStyle(wb, sheet = sheets[k], style = leftAlignStyle, rows = 2:nrow(data.frame(df_list[k])),cols = 1:ncol(data.frame(df_list[k])), gridExpand = TRUE)
    addStyle(wb, sheet = sheets[k], style = border, rows = 2:nrow(data.frame(df_list[k])),cols = 1:ncol(data.frame(df_list[k])), gridExpand = TRUE) # Punteado
    
    for (i in 2:(nrow(data.frame(df_list[k])) + 1)) {  # Rows 3 to nrow(data) + 2 (header is in row 2)
      if ((i %% 2) == 0) {
        
        # Apply white background to odd-numbered rows
        addStyle(wb, sheet = sheets[k], style = whiteStyle, rows = i, cols = 1:ncol(data.frame(df_list[k])), gridExpand = TRUE)
      } else {
        # Apply gray background to even-numbered rows
        addStyle(wb, sheet = sheets[k], style = grayStyle, rows = i, cols = 1:ncol(data.frame(df_list[k])), gridExpand = TRUE)
      }
    }
    
  }
  saveWorkbook(wb,file_xlsx,overwrite = TRUE)
  
}



#---------------------------------------------------------------------------------------
#Se agrega alguna imagen al inicio del formato
#---------------------------------------------------------------------------------------

worksheet_export2<-function(df,file_xlsx,sheets,fila,columna,Style,imagen,Titulos){
  
  df_list <- list()
  df_list[[1]] <- df
  
  wb <- createWorkbook()
  
  for (k in 1:length(sheets)){
    
    addWorksheet(wb, sheet = sheets[k])
    modifyBaseFont(wb, fontSize = 11, fontName = "Arial")
    
    writeDataTable(wb,
                   sheets[k],
                   data.frame(df_list[k]),
                   startRow = fila,
                   startCol = columna,
                   tableStyle = Style)
    
    #Set column widths auto
    setColWidths(wb,sheets[k],cols = 1:ncol(data.frame(df_list[k])),widths = "auto")
    
    #Set row widths auto
    #setRowHeights(wb, sheets[k], rows = 7:nrow(data.frame(df_list[k])), heights = 40)
    
    #Create a header style
    headerStyle <- createStyle(fontColour="white",fgFill="blue",halign="left",valign="center",textDecoration="bold")
    
    if (length(imagen)!=0 & length(Titulos)!=0) {
      
      #Create a body style
      #bodyStyle <- createStyle(border = "TopBottom", borderColour = "#4F81BD")
      #addStyle(wb, sheet = sheets[k], bodyStyle, rows = 7:nrow(data.frame(df_list[k])), cols = 1:ncol(data.frame(df_list[k])), gridExpand = TRUE)
      
      #Insert titles
      mergeCells(wb, sheets[k], rows=2,cols= 4:6)
      writeData(wb, sheets[k], Titulos[1], startCol = 4, startRow = 2)
      
      mergeCells(wb, sheets[k], rows=3,cols= 4:6)
      writeData(wb, sheets[k],Titulos[2], startCol = 4, startRow = 3)
      #addStyle(wb = wb, sheet = sheets[k], cols = 4L, rows = 1:4,style = createStyle(halign = 'right'))
      
      # Insert the image into the worksheet
      img_path <- imagen
      insertImage(wb, sheet = sheets[k], file = img_path, width = 1.8, height = 1, startRow = 1, startCol = 2)
    }
    
    
  }
  saveWorkbook(wb,file_xlsx,overwrite = TRUE)
  
}



#Referencias: https://rpubs.com/Emiliano_Montalvo/comparacion_proporciones

