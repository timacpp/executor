#include <deque>
#include <vector>
#include <unordered_map>

#include <atomic>
#include <mutex>
#include <thread>
#include <condition_variable>

#include <iostream>
#include <algorithm>
#include <sstream>

#include <stdio.h>
#include <cstdlib>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <fcntl.h>

namespace {
    template<typename T>
    using is_numeric = typename std::enable_if<std::is_arithmetic<T>::value, T>::type;

    template<typename AsType, typename = is_numeric<AsType>>
    AsType as(const std::string& numeric_string) {
        std::istringstream stream(numeric_string);
        AsType result;
        stream >> result;
        return result;
    }

    template<typename UnixReturnType, typename = is_numeric<UnixReturnType>>
    void unix_try(UnixReturnType expression_result, std::string&& hint) {
        if ((expression_result) == -1) {
            std::cerr << "Error in " << hint << std::endl;
            std::exit(1);
        }
    }
}

class Executor {
public:
    using millis_t = long;
    using task_id = size_t;

    struct Command {
        std::string name;
        std::vector<std::string> args;
    };

    struct Task {
        pid_t pid;
        std::string out;
        std::string err;
    };

    struct TaskResult {
        task_id id;
        int exit_code;
        bool signalled;
    };

    void start() {
        this->reader = std::move(std::thread([&]{ read_commands(); }));

        while (active) {
            clear_task_results();
            std::unique_lock<std::mutex> lock{command_mutex};

            command_available.wait(lock, [this]{ return !commands.empty(); });  // TODO: do not wait if can clear tasks

            Command command = commands.front();
            commands.pop_front();
            lock.unlock();

            execute(command);
        }
    }

private:
    static constexpr long MICROS_IN_MILLI = 1000;

    task_id next_task_id = 0;
    std::atomic<bool> active{true};
    std::deque<Command> commands;
    std::vector<TaskResult> results;
    std::unordered_map<task_id, Task> tasks;

    std::thread reader;
    std::mutex task_mutex;
    std::mutex command_mutex;
    std::mutex results_mutex;
    std::condition_variable command_available;
    std::condition_variable task_available;

    void execute(Command& command) {
        if (command.name == "run") {
            run(command.args);
        } else if (command.name == "out") {
            out(as<task_id>(command.args[0]));
        } else if (command.name == "err") {
            err(as<task_id>(command.args[0]));
        } else if (command.name == "kill") {
            kill(as<task_id>(command.args[0]));
        } else if (command.name == "sleep") {
            sleep(as<millis_t>(command.args[0]));
        } else if (command.name == "quit") {
            quit();
        } else {
            std::cerr << "Unknown command: " << command.name << "\n";
        }
    }

    void clear_task_results() {
        std::lock_guard<std::mutex> lock{results_mutex};

        for (TaskResult result : results) {
            if (result.signalled) {
                std::cout << "Task " << result.id << " ended: signalled.\n";
            } else {
                std::cout << "Task " << result.id << " ended: status " << result.exit_code << ".\n";
            }
        }

        results.clear();
    }

    void out(task_id id) {
        do_synchronized(id, [&](const auto& it) {
            if (it != tasks.end()) {
                std::cout << "Task " << id << " stdout: '" << it->second.out << "'.\n";
            } else {
                std::cerr << "Task " << id << " stdout: no such task.\n";
            }
        });
    }

    void err(task_id id) {
        do_synchronized(id, [&](const auto& it) {
            if (it != tasks.end()) {
                std::cout << "Task " << id << " stderr: '" << it->second.err << "'.\n";
            } else {
                std::cerr << "Task " << id << " stderr: no such task.\n";
            }
        });
    }

    void kill(task_id id) {
        do_synchronized(id, [&](const auto& it) {
            if (it != tasks.end()) {
                ::kill(it->second.pid, SIGINT); // TODO: join the worker thread?
            } else {
                std::cerr << "Task " << id << " kill: no such task.\n";
            }
        });
    }

    void do_synchronized(task_id id, const std::function<void(const decltype(tasks)::const_iterator&)>& action) {
        std::lock_guard<std::mutex> lock{task_mutex};
        action(tasks.find(id));
    }

    void sleep(millis_t millis) {
        usleep(millis * MICROS_IN_MILLI);
    }

    void quit() {
        for (auto& [id, _] : tasks) {
            kill(id);
        }

        active = false;
        reader.join();
    }

    void run(const std::vector<std::string>& args) {
        std::unique_lock<std::mutex> lock{task_mutex};
        const task_id current_task_id = next_task_id++;

        std::thread([&]{ start_task(current_task_id, args); }).detach();
        task_available.wait(lock, [&]{ return tasks.find(current_task_id) != tasks.end(); });

        std::cout << "Task " << current_task_id << " started: pid " << tasks[current_task_id].pid << ".\n";
    }

    void start_task(task_id id, const std::vector<std::string>& args) {
        int stdout_pipe[2], stderr_pipe[2];

        unix_try(pipe(stdout_pipe), "stdout pipe");
        unix_try(pipe(stderr_pipe), "stderr pipe");

        pid_t daemon_pid = create_daemon(args, stdout_pipe, stderr_pipe);
        initialize_task(id, daemon_pid);

        unix_try(close(stdout_pipe[1]), "close stdout write");
        unix_try(close(stderr_pipe[1]), "close stderr write");

        std::atomic<bool> task_active{true};
        std::thread stdout_reader([&]{ read_daemon_output(id, stdout_pipe[0], STDOUT_FILENO, task_active); });
        std::thread stderr_reader([&]{ read_daemon_output(id, stderr_pipe[0], STDERR_FILENO, task_active); });

        int status;
        wait(&status);

        task_active = false;
        stdout_reader.join();
        stderr_reader.join();

        save_task_result(id, status);
    }

    pid_t create_daemon(const std::vector<std::string>& args, int stdout_pipe[2], int stderr_pipe[2]) {
        pid_t pid = fork();
        unix_try(pid, "fork");

        if (pid != 0) {
            return pid;
        }

        const char* program = args[0].c_str();
        char* argv[args.size() + 1];

        for (size_t i = 0; i < args.size(); i++) {
            argv[i] = const_cast<char*>(args[i].c_str());
        }

        argv[args.size()] = nullptr;

        unix_try(close(stdout_pipe[0]), "close stdout read");
        unix_try(close(stderr_pipe[0]), "close stderr read");

        unix_try(dup2(stdout_pipe[1], STDOUT_FILENO), "dup2 stdout");
        unix_try(dup2(stderr_pipe[1], STDERR_FILENO), "dup2 stderr");

        unix_try(execvp(program, argv), "execv");
        exit(0);
    }

    void initialize_task(task_id id, pid_t pid) {
        std::lock_guard<std::mutex> lock{task_mutex};
        tasks[id] = {pid, "", ""};
        task_available.notify_one();
    }

    void save_task_result(task_id id, int status) {
        std::lock_guard<std::mutex> lock{results_mutex};

        if (WIFSIGNALED(status)) {
            results.push_back({id, 0, true});
        } else {
            results.push_back({id, WEXITSTATUS(status), false});
        }

        command_available.notify_one();
    }

    void read_daemon_output(task_id id, int input_fd, int output_fd, const std::atomic<bool>& task_active) {
        std::string message;

        while (task_active) {
            char buffer[1];
            ssize_t bytes_read = read(input_fd, buffer, 1);

            if (bytes_read > 0 && *buffer != '\n') { // TODO: update message when EOF
                message += *buffer;
                continue;
            }

            if (bytes_read == 0) {
                break;
            }

            std::lock_guard<std::mutex> lock{task_mutex};

            if (output_fd == STDOUT_FILENO) {
                tasks[id].out = message;
            } else {
                tasks[id].err = message;
            }

            message.clear();
        }
    }

    void read_commands() {
        std::string cli_command;

        while (cli_command != "quit" && std::getline(std::cin, cli_command)) {
            std::istringstream stream{cli_command};
            Command command;

            stream >> command.name;

            for (std::string arg; stream >> arg; ) {
                command.args.push_back(arg);
            }

            add_command(std::move(command));
        }

        if (cli_command != "quit") {
            add_command((Command) {.name = "quit"});
        }
    }

    void add_command(Command&& command) {
        std::lock_guard<std::mutex> lock{command_mutex};
        commands.emplace_back(command);
        command_available.notify_one();
    }
};

int main() {
    Executor().start();
    return 0;
}