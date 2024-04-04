import ballerina/http;
import ballerina/uuid;
import ballerinax/mongodb;

configurable string host = ?;
configurable string database = ?;
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

    resource function get slotmachinestats/[string email]() returns SlotMachineReport[]|error {
        mongodb:Collection smCollection = check self.Db->getCollection(slotMachineRecordsCollection);
        stream<SlotMachineReport, error?> resultStream = check smCollection->aggregate([
            {
                \$group: {
                    _id: null,
                    orig: {
                        \$push: "$$ROOT"
                    },
                    "total": {
                        \$sum: "$amount"
                    }
                }
            },
            {
                \$unwind: "$orig"
            },
            {
                \$project: {
                    date: "$orig.date",
                    amount: "$orig.amount",
                    email: "$orig.email",
                    total: "$total"
                }
            },
            {
                \$match: {email: email}
            },
            {
                \$group: {
                    _id: "$date",
                    amount: {
                        \$sum: "$amount"
                    },
                    orig: {
                        \$push: "$$ROOT.total"
                    }
                }
            },
            {
                "$unwind": "$orig"
            },
            {
                \$group: {
                    _id: {
                        _id: "$_id",
                        amount: "$amount"
                    }
                }
            },
            {
                \$project: {
                    date: "$_id._id",
                    "amount": "$_id.amount",
                    _id: 0
                }
            }
        ]);

        return from SlotMachineReport slms in resultStream
            select slms;
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
        mongodb:UpdateResult updateResult = check creditCol->updateOne({email}, {set: {amount: balance}});
        if updateResult.modifiedCount != 1 {
            return error(string `Failed to update the credits with email ${email}`);
        }
        SlotMachineRecord sm = check addSlotMachineRecord(self.Db, email, update.deduction, update.date);
        return getCredit(self.Db, email);
    }
}

isolated function getCredit(mongodb:Database Db, string email) returns Credit|error {
    mongodb:Collection creditCol = check Db->getCollection(creditCollection);
    stream<Credit, error?> findResult = check creditCol->find({email});
    Credit[] result = check from Credit m in findResult
        select m;
    if result.length() == 0 {
        string id = uuid:createType1AsString();
        Credit cr = {id: id, amount: 100, email: email};
        check creditCol->insertOne(cr);
        return cr;
    }
    return result[0];
}

isolated function addSlotMachineRecord(mongodb:Database Db, string email, int amount, string date) returns SlotMachineRecord|error {
    mongodb:Collection smCol = check Db->getCollection(slotMachineRecordsCollection);
    string id = uuid:createType1AsString();
    SlotMachineRecord sm = {id: id, amount: amount, email: email, date: date};
    check smCol->insertOne(sm);
    return sm;
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

public type SlotMachineReport record {|
    int amount;
    string date;
|};
