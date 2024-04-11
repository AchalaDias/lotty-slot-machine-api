import ballerina/http;
import ballerina/log;
import ballerina/oauth2;
import ballerina/sql;
import ballerina/uuid;
import ballerinax/mongodb;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

configurable string host = ?;
configurable string database = ?;
configurable string resultHost = ?;
configurable string tokenUrl = ?;
configurable string clientId = ?;
configurable string clientSecret = ?;

configurable string mysqlHost = ?;
configurable string mysqlUser = ?;
configurable string mysqlPassword = ?;
configurable int mysqlPort = ?;

configurable string dbType = ?;

const string creditCollection = "credits";
const string slotMachineRecordsCollection = "slot_machine_records";

final mongodb:Client mongoDb = check new ({
    connection: host
});

# A service representing a network-accessible API
# bound to port `9090`.
service / on new http:Listener(9090) {
    private final mongodb:Database Db;

    function init() returns error? {
        self.Db = check mongoDb->getDatabase(database);
    }

    resource function get getresults/[string email]() returns json|error {
        oauth2:ClientOAuth2Provider provider = new ({
            tokenUrl: tokenUrl,
            clientId: clientId,
            clientSecret: clientSecret,
            scopes: []
        });
        string token = "Bearer " + check provider.generateToken();
        log:printError("################################################");
        log:printError(token.toString());
        log:printError(resultHost);

        http:Client apiClient = check new (resultHost);
        http:Response response = check apiClient->get("/slotmachineresults/" + email, {"Authorization": token});

        log:printError("&&&&&&&&&& 1 " + check response.getTextPayload());
        log:printError("&&&&&&&&&& 2 " + response.resolvedRequestedURI.toString());
        log:printError("################################################");
        return response.getJsonPayload();

    }

    resource function get credits/[string email]() returns Credit|error {
        return getCredit(self.Db, email);
    }

    resource function put credits/[string email](CreditUpdate update) returns Credit|error {
        mongodb:Collection creditCol = check self.Db->getCollection(creditCollection);
        Credit currentCredit = check getCredit(self.Db, email);

        int balance = currentCredit.amount + update.deduction;
        if balance <= 0 {
            balance = 0;
        }
        if dbType == "mysql" {
            mysql:Client mysqlDb = check getMysqlConnection();
            sql:ParameterizedQuery query = `UPDATE Credits
                                        SET amount = ${balance}
                                        WHERE email = ${email}`;
            sql:ExecutionResult result = check mysqlDb->execute(query);

        } else {
            mongodb:UpdateResult updateResult = check creditCol->updateOne({email}, {set: {amount: balance}});
            if updateResult.modifiedCount != 1 {
                return error(string `Failed to update the credits with email ${email}`);
            }
        }
        SlotMachineRecord sm = check addSlotMachineRecord(self.Db, email, update.deduction, update.date);
        currentCredit.amount = balance;
        return currentCredit;
    }
}

isolated function getCredit(mongodb:Database Db, string email) returns Credit|error {
    string id = uuid:createType1AsString();
    Credit cr = {id: id, amount: 100, email: email};

    if dbType == "mysql" {
        mysql:Client mysqlDb = check getMysqlConnection();
        Credit|sql:Error credit = mysqlDb->queryRow(
        `SELECT * FROM Credits WHERE email = ${email}`);

        if credit is sql:NoRowsError {
            sql:ParameterizedQuery query = `INSERT INTO Credits(amount, email)
                                  VALUES (${cr.amount}, ${cr.email})`;
            sql:ExecutionResult result = check mysqlDb->execute(query);
            return cr;
        }
        return credit;
    } else {
        mongodb:Collection creditCol = check Db->getCollection(creditCollection);
        stream<Credit, error?> findResult = check creditCol->find({email});
        Credit[] result = check from Credit m in findResult
            select m;
        if result.length() == 0 {
            check creditCol->insertOne(cr);
            return cr;
        }
        return result[0];
    }
}

isolated function addSlotMachineRecord(mongodb:Database Db, string email, int amount, string date) returns SlotMachineRecord|error {
    string id = uuid:createType1AsString();
    SlotMachineRecord sm = {id: id, amount: amount, email: email, date: date};
    if dbType == "mysql" {
        mysql:Client mysqlDb = check getMysqlConnection();
        sql:ParameterizedQuery query = `INSERT INTO SlotMachineRecords(amount, date, email)
                                  VALUES (${sm.amount}, ${sm.date},${sm.email})`;
        sql:ExecutionResult|sql:Error result = mysqlDb->execute(query);
    } else {
        mongodb:Collection smCol = check Db->getCollection(slotMachineRecordsCollection);
        check smCol->insertOne(sm);
    }
    return sm;
}

isolated function getMysqlConnection() returns mysql:Client|sql:Error {
    final mysql:Client|sql:Error dbClient = new (
        host = mysqlHost, user = mysqlUser, password = mysqlPassword, port = mysqlPort, database = database
    );
    return dbClient;
}

public type CreditInput record {|
    int amount;
    string email;
|};

public type CreditUpdate record {|
    int deduction;
    string date;
|};

public type Credit record {|
    readonly string id;
    *CreditInput;
|};

public type SlotMachineRecordInput record {|
    int amount;
    string date;
    string email;
|};

public type SlotMachineRecord record {|
    readonly string id;
    *SlotMachineRecordInput;
|};

