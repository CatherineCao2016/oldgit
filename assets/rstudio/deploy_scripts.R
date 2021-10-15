input_table_name <-  Sys.getenv('input_table_name')
output_table_name <-  Sys.getenv('output_table_name')
library('fastDummies')
library(data.table)
library(caret)
library(zoo)
library(workflows)
library(glmnet)
# load data
library(ibmWatsonStudioLib)
wslib <- access_project_or_space()

Db2_WoC_metadata = wslib$get_connection("Db2 WoC")

library(RJDBC)

drv <- JDBC(driverClass="com.ibm.db2.jcc.DB2Driver", classPath="/opt/ibm/connectors/db2/db2jcc4.jar")

Db2_WoC_url <- paste("jdbc:db2://",
                     Db2_WoC_metadata[][["host"]],
                     ":", Db2_WoC_metadata[][["port"]],
                     "/", Db2_WoC_metadata[][["database"]],
                     ":", "sslConnection=true;",
                     sep=""
)

Db2_WoC_connection <- dbConnect(drv,
                                Db2_WoC_url,
                                Db2_WoC_metadata[][["username"]],
                                Db2_WoC_metadata[][["password"]]
)


query <- paste0("SELECT * FROM \"HMCRD\".\"", input_table_name, "\"")

data <- dbSendQuery(Db2_WoC_connection, query)

new_data <- dbFetch(data, n = 1000)

data_type <- data.frame(dbColumnInfo(data))
df1 <- as.data.frame(lapply(new_data[levels(factor(data_type$field.name[data_type$field.type == "DECFLOAT"]))], as.numeric))
df2 <- new_data[setdiff(data_type$field.name, data_type$field.name[data_type$field.type == "DECFLOAT"])]
dt1 <- cbind(df1, df2)


# Box-Cox transform
preProcValues <- preProcess(dt1, method = "BoxCox")
dt1_tran <- predict(preProcValues, dt1)

#Recreate numeric list with new dt1_tran
numeric_list <- unlist(lapply(dt1_tran, is.numeric))
dt1_num <- setDT(dt1_tran)[,..numeric_list]

dt1_num2 <- na.aggregate(dt1_num)

# Non-numeric
non_numeric_list <- unlist(lapply(dt1_tran, is.character))
dt1_non_num <- setDT(dt1_tran)[,..non_numeric_list]

# create dummies for non-numeric cols
#dt1_non_num <- cbind(dt1_non_num,dt1_tran[,'TARGET'])
dt1_non_num_dum <- dummy_cols(dt1_non_num, select_columns = colnames(dt1_non_num), remove_first_dummy = T)
colnames(dt1_non_num_dum)
# Attaching numeric and non numeric columns and handling missing values
dt1_preproc <- cbind(dt1_non_num_dum,dt1_num2)

dt1_preproc <- data.frame(dt1_preproc)
dt1_preproc[is.na(dt1_preproc)] <- 0

dt1_preproc_sample <- dt1_preproc

cols_to_keep <- c('FLAG_OWN_CAR_Y','ORGANIZATION_TYPE_Business.Entity.Type.1','DAYS_ID_PUBLISH','SK_ID_CURR','REG_CITY_NOT_LIVE_CITY','YEARS_BEGINEXPLUATATION_MODE','COMMONAREA_MODE','FLOORSMAX_MODE','LIVINGAPARTMENTS_MODE','YEARS_BUILD_MEDI','CODE_GENDER_M','OCCUPATION_TYPE_Waiters.barmen.staff','EXT_SOURCE_1','EXT_SOURCE_2','EXT_SOURCE_3')
dt1_preproc_sample <- dt1_preproc_sample[, cols_to_keep]

wslib$download_file("tidymodel.rds")
my_first_tidy_model <- readRDS("tidymodel.rds")

predictions <- predict(my_first_tidy_model, dt1_preproc_sample)
#predictions_prob <- predict(my_first_tidy_model, dt1_preproc_sample, type = "prob")

output_df <- as.data.frame(predictions)
colnames(output_df) <- "pred_class"

# save output
dbWriteTable(Db2_WoC_connection, output_table_name, output_df, overwrite=TRUE)

print("Output has been saved! Table Name =")
print(output_table_name)

