#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <memory>
#include <stdexcept>
#include <postgresql/libpq-fe.h>
#include <nlohmann/json.hpp>
#include <crow.h>
#include "PlainRpcDispatcher.h"
#include "LocationService.h"
#include <cstdlib>
#include <set>

using json = nlohmann::json;
using namespace std;

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
    DatabaseConnection(const DatabaseConnection&) = delete;
    DatabaseConnection& operator=(const DatabaseConnection&) = delete;
    PGconn* get() const { return conn_; }
    bool isValid() const { return conn_ && PQstatus(conn_) == CONNECTION_OK; }
};

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
        global_conninfo =
            "postgresql://postgres.vxqsqaysrpxliofqxjyu:the-plus-maps-password"
            "@aws-0-us-east-2.pooler.supabase.com:5432/postgres?sslmode=require";

        ensureDbConnection();

        auto dispatcher = make_shared<PlainRpcDispatcher>();
        dispatcher->registerMethod("getTopLocations", GetTopLocations);
        dispatcher->registerMethod("getLocationById", GetLocationById);
        dispatcher->registerMethod("searchLocations", SearchLocations);

        crow::SimpleApp app;

        // CORS middleware
        CROW_ROUTE(app, "/")
        .methods("OPTIONS"_method)
        ([](const crow::request& req, crow::response& res){
            res.add_header("Access-Control-Allow-Origin", "*");
            res.add_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
            res.add_header("Access-Control-Allow-Headers", "Content-Type, Authorization");
            res.add_header("Access-Control-Max-Age", "86400");
            res.end();
        });

        CROW_ROUTE(app, "/health")
        ([](){
            return "OK";
        });

        CROW_ROUTE(app, "/rpc")
        .methods("POST"_method)
        ([&dispatcher](const crow::request& req){
            crow::response res;
            res.add_header("Content-Type", "application/json");
            
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
                        res.body = json{{"success", false}, {"error", "You have exceeded the rate limit"}}.dump();
                        return res;
                    }
                    logUserRequest(db_connection->get(), userid);
                    if (isUserBlocked(db_connection->get(), userid)) {
                        res.code = 429;
                        res.body = json{{"success", false}, {"error", "You have exceeded the rate limit"}}.dump();
                        return res;
                    }
                }
            }

            json response = dispatcher->dispatch(request);
            if (!userid.empty()) logUserResponse(db_connection->get(), userid);
            res.body = response.dump();
            return res;
        });

        // Set CORS headers for all responses
        app.loglevel(crow::LogLevel::Warning);
        app.port(8080).multithreaded().run();

    } catch (const exception& e) {
        cerr << "[Fatal] " << e.what() << endl;
        return 1;
    }
    return 0;
}