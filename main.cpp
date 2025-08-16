#include "crow.h"
#include "PlainRpcDispatcher.h"
#include "LocationService.h"
#include <nlohmann/json.hpp>
#include <postgresql/libpq-fe.h>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <set>
#include <sstream>
#include <thread>
#include <chrono>

using json = nlohmann::json;
using namespace std;

// --- Database Connection Class (Unchanged) ---
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

// --- Global Variables ---
unique_ptr<DatabaseConnection> db_connection;
unique_ptr<LocationService> locationService;
string global_conninfo;

// --- Helper Functions (Mostly Unchanged) ---

bool ensureDbConnection(int retries = 5, int delayMs = 2000) {
    for (int i = 0; i < retries; i++) {
        if (!db_connection || !db_connection->isValid()) {
            try {
                db_connection = make_unique<DatabaseConnection>(global_conninfo.c_str());
                locationService = make_unique<LocationService>(db_connection->get());
                cout << "[DB] Re-connected to database.\n";
                return true;
            } catch (const exception& e) {
                cerr << "[DB] Connection attempt " << i + 1 << " failed: " << e.what() << endl;
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
    PGResultWrapper res(PQexecParams(conn, "SELECT log_user_request($1);", 1, nullptr, param, nullptr, nullptr, 0));
}

void logUserResponse(PGconn* conn, const string& userid) {
    const char* param[1] = { userid.c_str() };
    PGResultWrapper res(PQexecParams(conn, "SELECT log_user_response($1);", 1, nullptr, param, nullptr, nullptr, 0));
}

bool isUserBlocked(PGconn* conn, const string& userid) {
    const char* param[1] = { userid.c_str() };
    PGResultWrapper res(PQexecParams(conn, "SELECT is_user_blocked($1);", 1, nullptr, param, nullptr, nullptr, 0));
    if (PQresultStatus(res.get()) != PGRES_TUPLES_OK || PQntuples(res.get()) == 0) {
        return false;
    }
    bool blocked = (strcmp(PQgetvalue(res.get(), 0, 0), "t") == 0);
    return blocked;
}

// --- RPC Method Implementations (Unchanged) ---
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

// --- NEW: Crow CORS Middleware ---
struct CorsMiddleware {
    set<string> allowed_origins;

    CorsMiddleware() {
        const char* env_origins = getenv("ALLOWED_ORIGINS");
        // Default origins include your deployed app, localhost for typical web dev, and the default Vite dev server port.
        string origins_str = env_origins ? env_origins : "https://the-super-sweet-two.vercel.app,http://localhost:3000,http://127.0.0.1:5173";
        
        stringstream ss(origins_str);
        string origin;
        while (getline(ss, origin, ',')) {
            allowed_origins.insert(origin);
        }
    }

    struct context {};

    void before_handle(crow::request& req, crow::response& res, context& /*ctx*/) {
        string origin = req.get_header_value("Origin");
        if (!origin.empty() && allowed_origins.count(origin)) {
            res.set_header("Access-Control-Allow-Origin", origin);
            res.set_header("Vary", "Origin");
        }

        res.set_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        res.set_header("Access-Control-Allow-Headers", "Content-Type, Authorization");
        res.set_header("Access-Control-Max-Age", "86400");

        if (req.method == "OPTIONS"_method) {
            res.code = 204;
            res.end();
        }
    }

    void after_handle(crow::request& /*req*/, crow::response& /*res*/, context& /*ctx*/) {
        // No operation needed after handling
    }
};


// --- Main Application ---
int main() {
    try {
        global_conninfo =
            "postgresql://postgres.vxqsqaysrpxliofqxjyu:the-plus-maps-password"
            "@aws-0-us-east-2.pooler.supabase.com:5432/postgres?sslmode=require";
        
        // Establish initial DB connection
        cout << "[DB] Connecting to database..." << endl;
        if (!ensureDbConnection(5, 1000)) {
            cerr << "[Fatal] Could not establish initial database connection after multiple retries." << endl;
            return 1;
        }
        cout << "[DB] Initial database connection successful." << endl;

        auto dispatcher = make_shared<PlainRpcDispatcher>();
        dispatcher->registerMethod("getTopLocations", GetTopLocations);
        dispatcher->registerMethod("getLocationById", GetLocationById);
        dispatcher->registerMethod("searchLocations", SearchLocations);

        crow::SimpleApp app;
        CROW_MIDDLEWARE_REGISTER(app, CorsMiddleware);

        CROW_ROUTE(app, "/health").methods("GET"_method)
        ([](const crow::request&, crow::response& res) {
            res.set_header("Content-Type", "text/plain");
            res.write("OK");
            res.end();
        });

        CROW_ROUTE(app, "/rpc").methods("POST"_method)
        ([&dispatcher](const crow::request& req) -> crow::response {
            if (!ensureDbConnection(1, 0)) {
                return crow::response(503, json{{"success", false}, {"error", "Database unavailable"}}.dump());
            }

            json request_json;
            try {
                request_json = json::parse(req.body);
            } catch (const json::parse_error& e) {
                return crow::response(400, json{{"success", false}, {"error", "Invalid JSON body"}}.dump());
            }

            string userid;
            if (request_json.contains("params") && request_json["params"].contains("userid") && request_json["params"]["userid"].is_string()) {
                userid = request_json["params"]["userid"].get<string>();

                if (!userid.empty()) {
                    if (isUserBlocked(db_connection->get(), userid)) {
                        return crow::response(429, json{{"success", false}, {"error", "You have exceeded the rate limit"}}.dump());
                    }
                    logUserRequest(db_connection->get(), userid);
                }
            }

            json response_json = dispatcher->dispatch(request_json);
            if (!userid.empty()) {
                logUserResponse(db_connection->get(), userid);
            }
            
            crow::response res(response_json.dump());
            res.set_header("Content-Type", "application/json");
            return res;
        });

        app.not_found([](crow::request&, crow::response& res) {
            res.code = 404;
            res.set_header("Content-Type", "application/json");
            res.write(json{{"success", false}, {"error", "Not Found"}}.dump());
            res.end();
        });

        cout << "[Server] Crow server starting on http://0.0.0.0:8080" << endl;
        app.port(8080).multithreaded().run();

    } catch (const exception& e) {
        cerr << "[Fatal] " << e.what() << endl;
        return 1;
    }
    return 0;
}
