#include <deque>
#include <vector>
#include <unordered_map>

#include <mutex>
#include <thread>
#include <condition_variable>

#include <iostream>
#include <algorithm>
#include <sstream>

#include <cstdlib>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>

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
    void unix_check(UnixReturnType expression_result, std::string&& hint, bool suppress_print = false) {
        if ((expression_result) == -1) {
            if (!suppress_print) {
                std::cerr << "Error in " << hint << std::endl;
            }
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
        bool active;
        std::string out;
        std::string err;
        std::mutex mutex;

        Task() : pid{0}, active{false} {}

        Task(Task&& task) noexcept :
            pid{task.pid}, active{task.active},
            out{std::move(task.out)}, err{std::move(task.err)} {}
    };

    struct TaskResult {
        task_id id;
        int exit_code;
        bool signalled;
    };

    Executor() {
        for (task_id id = 0; id < MAX_TASKS; id++) {
            tasks.emplace_back((Task){});
        }
    }

    void start() {
        this->reader = std::move(std::thread(&Executor::read_commands, this));

        while (active) {
            std::unique_lock<std::mutex> lock{job_mutex};

            job_available.wait(lock, [this]{ return !commands.empty() || !results.empty(); });

            if (!results.empty()) {
                display_task_results();
                results.clear();
                lock.unlock();
            } else {
                Command command = commands.front();
                commands.pop_front();
                lock.unlock();
                execute(std::move(command));
            }
        }
    }

private:
    static constexpr long MICROS_IN_MILLI = 1000;
    static constexpr size_t MAX_TASKS = 4096 + 7;

    bool active = true;
    std::thread reader;
    std::mutex job_mutex;
    std::condition_variable job_available;
    std::deque<Command> commands;
    std::vector<TaskResult> results;

    std::mutex run_mutex;
    task_id next_task_id = 0;
    pid_t current_daemon_pid = 0;
    std::condition_variable task_available;
    std::vector<Task> tasks{MAX_TASKS};
    std::vector<std::thread> workers;

    void execute(Command&& command) {
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
            std::cerr << "Unknown command: " << command.name << std::endl;
        }
    }

    void display_task_results() {
        for (TaskResult result : results) {
            if (result.signalled) {
                std::cout << "Task " << result.id << " ended: signalled." << std::endl;
            } else {
                std::cout << "Task " << result.id << " ended: status " << result.exit_code << "." << std::endl;
            }
        }
    }

    void out(task_id id) {
        std::lock_guard<std::mutex> lock{tasks[id].mutex};
        std::cout << "Task " << id << " stdout: '" << tasks[id].out << "'." << std::endl;
    }

    void err(task_id id) {
        std::lock_guard<std::mutex> lock{tasks[id].mutex};
        std::cout << "Task " << id << " stderr: '" << tasks[id].err << "'." << std::endl;
    }

    void kill(task_id id, int signal = SIGINT) {
        std::lock_guard<std::mutex> lock{tasks[id].mutex};
        if (tasks[id].active) {
            ::kill(tasks[id].pid, signal);
        }
    }

    void sleep(millis_t millis) {
        usleep(millis * MICROS_IN_MILLI);
    }

    void quit() {
        active = false;
        reader.join();

        for (task_id id = 0; id < tasks.size(); id++) {
            kill(id, SIGKILL);
        }

        for (auto& worker : workers) {
            worker.join();
        }

        display_task_results();
    }

    void run(const std::vector<std::string>& args) {
        set_current_daemon_pid(0, false);

        std::unique_lock<std::mutex> lock{run_mutex};
        workers.emplace_back(&Executor::start_task, this, next_task_id, args);
        task_available.wait(lock, [&]{ return current_daemon_pid != 0; });

        std::cout << "Task " << next_task_id << " started: pid " << current_daemon_pid << "." << std::endl;
        next_task_id++;
    }

    void start_task(task_id id, const std::vector<std::string>& args) {
        int stdout_pipe[2], stderr_pipe[2];

        unix_check(pipe(stdout_pipe), "stdout pipe");
        unix_check(pipe(stderr_pipe), "stderr pipe");

        pid_t daemon_pid = create_daemon(args, stdout_pipe, stderr_pipe);
        activate_task(id, daemon_pid);
        set_current_daemon_pid(daemon_pid, true);

        unix_check(close(stdout_pipe[1]), "close stdout write");
        unix_check(close(stderr_pipe[1]), "close stderr write");

        auto [stdout_reader, stderr_reader] = create_pipe_readers(id, stdout_pipe[0], stderr_pipe[0]);

        int status;
        waitpid(daemon_pid, &status, 0);

        stdout_reader.join();
        stderr_reader.join();

        save_task_result(id, status);
    }

    pid_t create_daemon(const std::vector<std::string>& args, int stdout_pipe[2], int stderr_pipe[2]) {
        pid_t pid = fork();
        unix_check(pid, "fork");

        if (pid != 0) {
            return pid;
        }

        const char* program = args[0].c_str();
        char* argv[args.size() + 1];

        for (size_t i = 0; i < args.size(); i++) {
            argv[i] = const_cast<char*>(args[i].c_str());
        }

        argv[args.size()] = nullptr;

        unix_check(dup2(stdout_pipe[1], STDOUT_FILENO), "dup2 stdout", true);
        unix_check(dup2(stderr_pipe[1], STDERR_FILENO), "dup2 stderr", true);

        unix_check(close(stdout_pipe[0]), "close stdout read", true);
        unix_check(close(stderr_pipe[0]), "close stderr read", true);
        unix_check(close(stdout_pipe[1]), "close stdout write", true);
        unix_check(close(stderr_pipe[1]), "close stderr read", true);

        unix_check(execvp(program, argv), "execvp", true);

        exit(0);
    }

    std::pair<std::thread, std::thread> create_pipe_readers(task_id id, int stdout_input, int stderr_input) {
        std::lock_guard<std::mutex> lock{job_mutex};
        return {
            std::thread(&Executor::read_task_output, this, id, stdout_input, STDOUT_FILENO),
            std::thread(&Executor::read_task_output, this, id, stderr_input, STDERR_FILENO)
        };
    }

    void activate_task(task_id id, pid_t pid) {
        std::unique_lock<std::mutex> lock{tasks[id].mutex};
        tasks[id].active = true;
        tasks[id].pid = pid;
    }

    void set_current_daemon_pid(pid_t pid, bool notify) {
        std::unique_lock<std::mutex> lock{run_mutex};
        current_daemon_pid = pid;
        if (notify) {
            task_available.notify_one();
        }
    }

    void save_task_result(task_id id, int status) {
        std::lock_guard<std::mutex> lock{job_mutex};

        if (WIFSIGNALED(status)) {
            results.push_back({id, WEXITSTATUS(status), true});
        } else {
            results.push_back({id, WEXITSTATUS(status), false});
        }

        job_available.notify_one();
    }

    void read_task_output(task_id id, int input_fd, int output_fd) {
        std::string message;

        while (true) {
            char byte;
            ssize_t bytes_read = read(input_fd, &byte, 1);

            unix_check(bytes_read, "read", true);

            if (bytes_read == 0) {
                if (!message.empty()) {
                    update_task_output(id, message, output_fd);
                }
                break;
            } else if (byte == '\n') {
                update_task_output(id, message, output_fd);
                message.clear();
            } else {
                message += byte;
            }
        }
    }

    void update_task_output(task_id id, const std::string& message, int output_fd) {
        std::lock_guard<std::mutex> lock{tasks[id].mutex};

        if (output_fd == STDOUT_FILENO) {
            tasks[id].out = message;
        } else {
            tasks[id].err = message;
        }
    }

    void read_commands() {
        std::string cli_command;

        while (cli_command != "quit" && std::getline(std::cin, cli_command)) {
            if (cli_command.empty()) {
                continue;
            }

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
        std::lock_guard<std::mutex> lock{job_mutex};
        commands.emplace_back(command);
        job_available.notify_one();
    }
};

int main() {
    Executor().start();
    return 0;
}