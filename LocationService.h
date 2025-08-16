
#ifndef LOCATION_SERVICE_H
#define LOCATION_SERVICE_H

#include <postgresql/libpq-fe.h>
#include <string>
#include <vector>
#include <stdexcept>
#include <memory>
#include <mutex>

// The data structure for a location, matching the database schema.
struct Location {
    std::string id;
    std::string name;
    std::string country;
    std::string state;
    std::string description;
    std::string svg_link;
    double rating;
};

// RAII wrapper for PGresult to ensure memory is always freed.
class PGResultWrapper {
private:
    PGresult* result_;
public:
    explicit PGResultWrapper(PGresult* result) : result_(result) {}
    ~PGResultWrapper() {
        if (result_) PQclear(result_);
    }
    PGResultWrapper(const PGResultWrapper&) = delete;
    PGResultWrapper& operator=(const PGResultWrapper&) = delete;
    PGresult* get() const { return result_; }
};

// Service class to interact with the database.
class LocationService {
private:
    PGconn* conn;
    mutable std::mutex db_mutex_;
    std::string sanitizeString(const std::string& input) const;
    Location rowToLocation(const PGResultWrapper& res, int row) const;

public:
    LocationService(PGconn* connection);
    ~LocationService();

    // Location methods
    std::vector<Location> getTopLocations(int limit);
    Location getLocationById(const std::string& id);
    std::vector<Location> searchLocations(const std::string& query);
};

#endif