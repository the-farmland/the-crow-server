#include <crow.h>
#include <crow/middlewares/cors.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <postgresql/libpq-fe.h>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <chrono>

using json = nlohmann::json;
using namespace std;

// Location data structure
struct Location {
    string id;
    string name;
    string country;
    string state;
    string description;
    string svg_link;
    double rating;
};

// Database connection wrapper
class DatabaseConnection {
private:
    PGconn* conn_;
public:
    explicit DatabaseConnection(const char* conninfo) {
        conn_ = PQconnectdb(conninfo);
        if (PQstatus(conn_) != CONNECTION_OK) {
            string error = PQerrorMessage(conn_);
            PQfinish(conn_);
            conn_ = nullptr;
            throw runtime_error("Database connection failed: " + error);
        }
    }
    ~DatabaseConnection() { if (conn_) PQfinish(conn_); }
    PGconn* get() const { return conn_; }
    bool isValid() const { return conn_ && PQstatus(conn_) == CONNECTION_OK; }
};

// Location service
class LocationService {
private:
    PGconn* conn;
    mutable mutex db_mutex_;
    
    string sanitizeString(const string& input) const {
        string sanitized = input;
        sanitized.erase(remove_if(sanitized.begin(), sanitized.end(),
            [](unsigned char c) { return c < 32 && c != '\t' && c != '\n' && c != '\r'; }),
            sanitized.end());
        return sanitized;
    }

    Location rowToLocation(PGresult* res, int row) const {
        return Location{
            .id = sanitizeString(PQgetvalue(res, row, 0) ? PQgetvalue(res, row, 0) : ""),
            .name = sanitizeString(PQgetvalue(res, row, 1) ? PQgetvalue(res, row, 1) : ""),
            .country = sanitizeString(PQgetvalue(res, row, 2) ? PQgetvalue(res, row, 2) : ""),
            .state = sanitizeString(PQgetvalue(res, row, 3) ? PQgetvalue(res, row, 3) : ""),
            .description = sanitizeString(PQgetvalue(res, row, 4) ? PQgetvalue(res, row, 4) : ""),
            .svg_link = sanitizeString(PQgetvalue(res, row, 5) ? PQgetvalue(res, row, 5) : ""),
            .rating = PQgetvalue(res, row, 6) ? stod(PQgetvalue(res, row, 6)) : 0.0
        };
    }

public:
    LocationService(PGconn* connection) : conn(connection) {
        if (!conn) throw runtime_error("Invalid database connection");
    }

    vector<Location> getTopLocations(int limit) {
        lock_guard<mutex> lock(db_mutex_);
        string query = "SELECT * FROM get_top_locations($1);";
        string limitStr = to_string(limit);
        const char* paramValues[1] = {limitStr.c_str()};

        PGresult* res = PQexecParams(conn, query.c_str(), 1, nullptr, paramValues, nullptr, nullptr, 0);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            PQclear(res);
            throw runtime_error("Query failed: " + string(PQerrorMessage(conn)));
        }

        vector<Location> locations;
        int rows = PQntuples(res);
        for (int i = 0; i < rows; i++) {
            locations.push_back(rowToLocation(res, i));
        }
        PQclear(res);
        return locations;
    }

    Location getLocationById(const string& id) {
        lock_guard<mutex> lock(db_mutex_);
        string query = "SELECT * FROM get_location_by_id($1);";
        const char* paramValues[1] = {id.c_str()};

        PGresult* res = PQexecParams(conn, query.c_str(), 1, nullptr, paramValues, nullptr, nullptr, 0);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            PQclear(res);
            throw runtime_error("Query failed: " + string(PQerrorMessage(conn)));
        }
        if (PQntuples(res) == 0) {
            PQclear(res);
            throw runtime_error("Location not found");
        }
        Location loc = rowToLocation(res, 0);
        PQclear(res);
        return loc;
    }

    vector<Location> searchLocations(const string& query) {
        lock_guard<mutex> lock(db_mutex_);
        string sqlQuery = "SELECT * FROM search_locations($1);";
        const char* paramValues[1] = {query.c_str()};

        PGresult* res = PQexecParams(conn, sqlQuery.c_str(), 1, nullptr, paramValues, nullptr, nullptr, 0);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            PQclear(res);
            throw runtime_error("Query failed: " + string(PQerrorMessage(conn)));
        }

        vector<Location> locations;
        int rows = PQntuples(res);
        for (int i = 0; i < rows; i++) {
            locations.push_back(rowToLocation(res, i));
        }
        PQclear(res);
        return locations;
    }
};

// Global instances
unique_ptr<DatabaseConnection> db_connection;
unique_ptr<LocationService> locationService;
string global_conninfo;

bool ensureDbConnection(int retries = 5, int delayMs = 2000) {
    for (int i = 0; i < retries; i++) {
        if (!db_connection || !db_connection->isValid()) {
            try {
                db_connection = make_unique<DatabaseConnection>(global_conninfo.c_str());
                locationService = make_unique<LocationService>(db_connection->get());
                cout << "[DB] Connected to database.\n";
                return true;
            } catch (const exception& e) {
                cerr << "[DB] Connection attempt " << i+1 << " failed: " << e.what() << endl;
                this_thread::sleep_for(chrono::milliseconds(delayMs));
            }
        } else {
            return true;
        }
    }
    return false;
}

json locationToJson(const Location& loc) {
    return json{
        {"id", loc.id},
        {"name", loc.name},
        {"country", loc.country},
        {"state", loc.state},
        {"description", loc.description},
        {"svg_link", loc.svg_link},
        {"rating", loc.rating}
    };
}

void logUserRequest(PGconn* conn, const string& userid) {
    const char* param[1] = { userid.c_str() };
    PGresult* res = PQexecParams(conn, "SELECT log_user_request($1);", 1, nullptr, param, nullptr, nullptr, 0);
    PQclear(res);
}

void logUserResponse(PGconn* conn, const string& userid) {
    const char* param[1] = { userid.c_str() };
    PGresult* res = PQexecParams(conn, "SELECT log_user_response($1);", 1, nullptr, param, nullptr, nullptr, 0);
    PQclear(res);
}

bool isUserBlocked(PGconn* conn, const string& userid) {
    const char* param[1] = { userid.c_str() };
    PGresult* res = PQexecParams(conn, "SELECT is_user_blocked($1);", 1, nullptr, param, nullptr, nullptr, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        PQclear(res);
        return false;
    }
    bool blocked = (strcmp(PQgetvalue(res, 0, 0), "t") == 0);
    PQclear(res);
    return blocked;
}

// RPC Dispatcher
class PlainRpcDispatcher {
private:
    using MethodHandler = function<json(const json&)>;
    unordered_map<string, MethodHandler> methods_;

public:
    void registerMethod(const string& method, MethodHandler handler) {
        if (methods_.find(method) != methods_.end()) {
            throw runtime_error("Method already registered");
        }
        methods_[method] = handler;
    }

    json dispatch(const json& request) {
        if (!request.contains("method") || !request["method"].is_string()) {
            throw runtime_error("Invalid request: missing method");
        }
        if (!request.contains("params") || !request["params"].is_object()) {
            throw runtime_error("Invalid request: missing params");
        }

        string method = request["method"];
        auto it = methods_.find(method);
        if (it == methods_.end()) {
            throw runtime_error("Method not found");
        }

        try {
            return it->second(request["params"]);
        } catch (const exception& e) {
            return json{{"success", false}, {"error", e.what()}};
        }
    }
};

// RPC Methods
json GetTopLocations(const json& params) {
    int limit = params.value("limit", 10);
    auto locations = locationService->getTopLocations(limit);
    json arr = json::array();
    for (const auto& loc : locations) arr.push_back(locationToJson(loc));
    return {{"success", true}, {"data", arr}};
}

json GetLocationById(const json& params) {
    if (!params.contains("id") || !params["id"].is_string())
        throw runtime_error("Invalid or missing 'id'");
    auto loc = locationService->getLocationById(params["id"].get<string>());
    return {{"success", true}, {"data", locationToJson(loc)}};
}

json SearchLocations(const json& params) {
    if (!params.contains("query") || !params["query"].is_string())
        throw runtime_error("Invalid or missing 'query'");
    auto results = locationService->searchLocations(params["query"].get<string>());
    json arr = json::array();
    for (const auto& loc : results) arr.push_back(locationToJson(loc));
    return {{"success", true}, {"data", arr}};
}

int main() {
    try {
        // Initialize database
        global_conninfo =           
            "postgresql://postgres.vxqsqaysrpxliofqxjyu:the-plus-maps-password"
            "@aws-0-us-east-2.pooler.supabase.com:5432/postgres?sslmode=require";
        ensureDbConnection();

        // Set up RPC methods
        auto dispatcher = make_shared<PlainRpcDispatcher>();
        dispatcher->registerMethod("getTopLocations", GetTopLocations);
        dispatcher->registerMethod("getLocationById", GetLocationById);
        dispatcher->registerMethod("searchLocations", SearchLocations);

        // Configure Crow with CORS
        crow::App<crow::CORSHandler> app;
        
        // Ultra-strict CORS configuration
        auto& cors = app.get_middleware<crow::CORSHandler>();
        cors
            .global()
            .headers("Content-Type", "Authorization", "X-Requested-With")
            .methods("POST"_method, "GET"_method, "OPTIONS"_method)
            .origin("*")
            .max_age(86400);

        // Health endpoint
        CROW_ROUTE(app, "/health")([](){
            return crow::response(200, "OK");
        });

        // Main RPC endpoint
        CROW_ROUTE(app, "/rpc")
            .methods("POST"_method, "OPTIONS"_method)
        ([&dispatcher](const crow::request& req){
            if (req.method == "OPTIONS"_method) {
                crow::response res(204);
                res.set_header("Access-Control-Allow-Origin", "*");
                return res;
            }

            crow::response res;
            res.set_header("Content-Type", "application/json");
            res.set_header("Access-Control-Allow-Origin", "*");

            if (!ensureDbConnection()) {
                res.code = 503;
                res.body = json{{"success", false}, {"error", "Database unavailable"}}.dump();
                return res;
            }

            json request;
            try {
                request = json::parse(req.body);
            } catch (...) {
                res.code = 400;
                res.body = json{{"success", false}, {"error", "Invalid JSON"}}.dump();
                return res;
            }

            string userid;
            if (request["params"].contains("userid") && request["params"]["userid"].is_string()) {
                userid = request["params"]["userid"].get<string>();

                if (!userid.empty()) {
                    if (isUserBlocked(db_connection->get(), userid)) {
                        res.code = 429;
                        res.body = json{{"success", false}, {"error", "Rate limit exceeded"}}.dump();
                        return res;
                    }
                    logUserRequest(db_connection->get(), userid);
                    if (isUserBlocked(db_connection->get(), userid)) {
                        res.code = 429;
                        res.body = json{{"success", false}, {"error", "Rate limit exceeded"}}.dump();
                        return res;
                    }
                }
            }

            json response = dispatcher->dispatch(request);
            if (!userid.empty()) logUserResponse(db_connection->get(), userid);
            res.body = response.dump();
            return res;
        });

        // Start server
        cout << "Server running on port 8080\n";
        app.port(8080).multithreaded().run();

    } catch (const exception& e) {
        cerr << "Fatal error: " << e.what() << endl;
        return 1;
    }
    return 0;
}