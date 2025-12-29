
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <sstream>
#include <locale>
#include <codecvt>
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <cmath>
#include <map>

#include <mongocxx/v_noabi/mongocxx/client.hpp>
#include <mongocxx/v_noabi/mongocxx/instance.hpp>
#include <mongocxx/v_noabi/mongocxx/uri.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>

#include "CustomContainers.hpp" 

using DocID = std::string; 


std::wstring utf8_to_wstring(const std::string& str) {
    std::wstring_convert<std::codecvt_utf8<wchar_t>> conv;
    return conv.from_bytes(str);
}

std::string wstring_to_utf8(const std::wstring& wstr) {
    std::wstring_convert<std::codecvt_utf8<wchar_t>> conv;
    return conv.to_bytes(wstr);
}

bool is_russian_letter(wchar_t c) {
    return (c >= L'а' && c <= L'я') ||
           (c >= L'А' && c <= L'Я') ||
           c == L'ё' || c == L'Ё';
}

std::vector<std::wstring> tokenize_ru(const std::string& text) {
    std::wstring wtext = utf8_to_wstring(text);

    for (auto& c : wtext)
        c = towlower(c);

    std::vector<std::wstring> tokens;
    std::wstring current;

    for (wchar_t c : wtext) {
        if (is_russian_letter(c)) {
            current += c;
        } else if (!current.empty()) {
            tokens.push_back(current);
            current.clear();
        }
    }

    if (!current.empty())
        tokens.push_back(current);

    return tokens;
}

const std::vector<std::wstring> endings = {
    L"иями", L"ями", L"ами",
    L"ией", L"ий", L"ый", L"ой",
    L"ия", L"ья", L"ие", L"ье",
    L"ых", L"ую", L"юю",
    L"ая", L"яя",
    L"ом", L"ем",
    L"ах", L"ях",
    L"ы", L"и", L"а", L"я", L"о", L"е", L"у", L"ю"
};

std::wstring stem_ru(const std::wstring& word) {
    if (word.length() <= 3)
        return word;

    for (const auto& end : endings) {
        if (word.length() > end.length() + 2 &&
            word.compare(word.length() - end.length(), end.length(), end) == 0) {
            return word.substr(0, word.length() - end.length());
        }
    }
    return word;
}

using InvertedIndex = Custom::HashMap<std::wstring, Custom::HashSet<DocID>>;


Custom::HashSet<DocID> set_and(
    const Custom::HashSet<DocID>& a,
    const Custom::HashSet<DocID>& b
) {
    Custom::HashSet<DocID> r;
    for (auto& x : a)
        if (b.contains(x)) r.insert(x); 
    return r;
}

Custom::HashSet<DocID> set_or(
    const Custom::HashSet<DocID>& a,
    const Custom::HashSet<DocID>& b
) {
    auto r = a;
    for (const auto& id : b) {
        r.insert(id);
    }
    return r;
}

Custom::HashSet<DocID> boolean_search_ru(const std::string& query, InvertedIndex& index) {
    auto tokens = tokenize_ru(query);
    Custom::HashSet<DocID> result;
    bool first = true;
    std::wstring op = L"and";

    for (auto& token : tokens) {
        if (token == L"and" || token == L"or" || token == L"not") {
            op = token;
            continue;
        }

        auto stem = stem_ru(token);
        auto docs = index[stem]; 

        if (first) {
            result = docs;
            first = false;
            continue;
        }

        if (op == L"and") result = set_and(result, docs);
        else if (op == L"or")  result = set_or(result, docs);
        else if (op == L"not") {
            for (auto& d : docs)
                result.erase(d);
        }
    }
    return result;
}

int main() {
    mongocxx::instance instance{};
    mongocxx::client client{ mongocxx::uri{"mongodb://localhost:27017"} };

    auto db = client["russian_corpus"];
    auto collection = db["parsed_pages"];

    InvertedIndex index;
    Custom::HashMap<std::wstring, size_t> term_frequencies; 

    size_t total_tokens = 0;
    size_t total_chars = 0;
    size_t total_bytes = 0;
    size_t docs_count = 0;

    std::cout << "Индексация документов и сбор статистики:\n";

    auto cursor = collection.find({}); 

    auto start_time = std::chrono::high_resolution_clock::now();

    for (auto&& doc : cursor) {
        DocID id = doc["_id"].get_oid().value.to_string();
        
        std::string text = std::string(doc["clean_text"].get_string().value);
        total_bytes += text.size();

        auto tokens = tokenize_ru(text);
        for (auto& t : tokens) {
            auto stem = stem_ru(t);
            index[stem].insert(id);
            
            term_frequencies[stem]++; 
            total_tokens++;
            total_chars += t.length();
        }
        docs_count++;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end_time - start_time;

    std::cout << "Данные для отчета:\n";
    std::cout << "Количество документов: " << docs_count << "\n";
    std::cout << "Общий объем текста: " << std::fixed << std::setprecision(2) << total_bytes / 1024.0 << " КБ\n";
    std::cout << "Общее количество токенов: " << total_tokens << "\n";
    std::cout << "Средняя длина токена: " << (total_tokens > 0 ? (double)total_chars / total_tokens : 0) << " симв.\n";
    std::cout << "Время индексации: " << elapsed.count() << " сек.\n";
    std::cout << "Скорость токенизации: " << (elapsed.count() > 0 ? (total_bytes / 1024.0) / elapsed.count() : 0) << " КБ/сек.\n";
    std::cout <<  "\n\n";

    std::vector<size_t> freqs;
    for (auto const& node : term_frequencies) {
        freqs.push_back(node.value);
    }

    std::sort(freqs.rbegin(), freqs.rend());

    std::cout << "Топ-10 частот для проверки (Ранг | Частота):\n";
    for (size_t i = 0; i < freqs.size(); ++i) {
        std::cout << i + 1 << " | " << freqs[i] << "\n";
    }

    while (true) {
        std::cout << "\nВведите булев запрос (или exit): ";
        
        std::string query;
        std::getline(std::cin, query);

        if (query == "exit") break;

        auto result = boolean_search_ru(query, index);
        std::cout << "Найдено документов: " << result.size() << "\n";

        for (auto& id : result) {
            auto doc = collection.find_one(
                bsoncxx::builder::basic::make_document(
                    bsoncxx::builder::basic::kvp("_id", bsoncxx::oid{id})
                )
            );
            if (doc) {
                std::cout << "- " << std::string(doc->view()["url"].get_string().value) << "\n";
            }
        }
    }

    return 0;
}
