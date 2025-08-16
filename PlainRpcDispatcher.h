

#pragma once
#include <functional>
#include <unordered_map>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

class PlainRpcDispatcher {
public:
    using MethodHandler = std::function<json(const json&)>;
    
    void registerMethod(const std::string& method, MethodHandler handler) {
        if (methods_.find(method) != methods_.end()) {
            throw std::runtime_error("Method '" + method + "' already registered");
        }
        methods_[method] = handler;
    }
    
    json dispatch(const json& request) {
        // Validate request structure
        if (!request.contains("method") || !request["method"].is_string()) {
            throw std::runtime_error("Invalid request: missing or invalid 'method' field");
        }
        
        if (!request.contains("params") || !request["params"].is_object()) {
            throw std::runtime_error("Invalid request: missing or invalid 'params' field");
        }

        std::string method = request["method"].get<std::string>();
        const json& params = request["params"];

        // Find and execute the method
        auto it = methods_.find(method);
        if (it == methods_.end()) {
            throw std::runtime_error("Method '" + method + "' not found");
        }

        try {
            // Call the registered method handler
            return it->second(params);
        } catch (const std::exception& e) {
            // Wrap any exceptions in a JSON error response
            return json{
                {"success", false},
                {"error", e.what()}
            };
        }
    }
    
private:
    std::unordered_map<std::string, MethodHandler> methods_;
};