
#include "LocationService.h"
#include <algorithm>
#include <stdexcept>

LocationService::LocationService(PGconn* connection) : conn(connection) {
    if (!conn) {
        throw std::runtime_error("Invalid database connection provided to LocationService.");
    }
}

LocationService::~LocationService() {}

// Helper to sanitize strings before database use.
std::string LocationService::sanitizeString(const std::string& input) const {
    std::string sanitized = input;
    sanitized.erase(std::remove_if(sanitized.begin(), sanitized.end(),
        [](unsigned char c) { return c < 32 && c != '\t' && c != '\n' && c != '\r'; }),
        sanitized.end());
    return sanitized;
}

// Helper to convert a database row into a Location struct.
Location LocationService::rowToLocation(const PGResultWrapper& res, int i) const {
    return Location{
        .id = sanitizeString(PQgetvalue(res.get(), i, 0) ? PQgetvalue(res.get(), i, 0) : ""),
        .name = sanitizeString(PQgetvalue(res.get(), i, 1) ? PQgetvalue(res.get(), i, 1) : ""),
        .country = sanitizeString(PQgetvalue(res.get(), i, 2) ? PQgetvalue(res.get(), i, 2) : ""),
        .state = sanitizeString(PQgetvalue(res.get(), i, 3) ? PQgetvalue(res.get(), i, 3) : ""),
        .description = sanitizeString(PQgetvalue(res.get(), i, 4) ? PQgetvalue(res.get(), i, 4) : ""),
        .svg_link = sanitizeString(PQgetvalue(res.get(), i, 5) ? PQgetvalue(res.get(), i, 5) : ""),
        .rating = PQgetvalue(res.get(), i, 6) ? std::stod(PQgetvalue(res.get(), i, 6)) : 0.0
    };
}

std::vector<Location> LocationService::getTopLocations(int limit) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    std::string query = "SELECT * FROM get_top_locations($1);";
    std::string limitStr = std::to_string(limit);
    const char* paramValues[1] = {limitStr.c_str()};

    PGResultWrapper res(PQexecParams(conn, query.c_str(), 1, nullptr, paramValues, nullptr, nullptr, 0));
    if (PQresultStatus(res.get()) != PGRES_TUPLES_OK) {
        throw std::runtime_error("Query failed: " + std::string(PQerrorMessage(conn)));
    }

    std::vector<Location> locations;
    int rows = PQntuples(res.get());
    locations.reserve(rows);
    for (int i = 0; i < rows; i++) {
        locations.push_back(rowToLocation(res, i));
    }
    return locations;
}

Location LocationService::getLocationById(const std::string& id) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    std::string sanitizedId = sanitizeString(id);
    std::string query = "SELECT * FROM get_location_by_id($1);";
    const char* paramValues[1] = {sanitizedId.c_str()};

    PGResultWrapper res(PQexecParams(conn, query.c_str(), 1, nullptr, paramValues, nullptr, nullptr, 0));
    if (PQresultStatus(res.get()) != PGRES_TUPLES_OK) {
        throw std::runtime_error("Query failed: " + std::string(PQerrorMessage(conn)));
    }
    if (PQntuples(res.get()) == 0) {
        throw std::runtime_error("Location not found");
    }
    return rowToLocation(res, 0);
}

std::vector<Location> LocationService::searchLocations(const std::string& queryStr) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    std::string sanitizedQuery = sanitizeString(queryStr);
    std::string sqlQuery = "SELECT * FROM search_locations($1);";
    const char* paramValues[1] = {sanitizedQuery.c_str()};

    PGResultWrapper res(PQexecParams(conn, sqlQuery.c_str(), 1, nullptr, paramValues, nullptr, nullptr, 0));
    if (PQresultStatus(res.get()) != PGRES_TUPLES_OK) {
        throw std::runtime_error("Query failed: " + std::string(PQerrorMessage(conn)));
    }

    std::vector<Location> locations;
    int rows = PQntuples(res.get());
    locations.reserve(rows);
    for (int i = 0; i < rows; i++) {
        locations.push_back(rowToLocation(res, i));
    }
    return locations;
}