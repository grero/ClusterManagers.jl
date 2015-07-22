# ClusterManager for HTCondor

export HTCManager, addprocs_htc

immutable HTCManager <: ClusterManager
end

function condor_script(portnum::Integer, np::Integer, config::Dict)
    exehome = config[:dir]
    exename = config[:exename]
    exeflags = config[:exeflags]
    #TODO: make this a configurable option
    priority = 15
	if "HOME" in keys(ENV)
		home = ENV["HOME"]
	else
		home = homedir()
	end
	if "HOSTNAME" in keys(ENV)
		hostname = ENV["HOSTNAME"]
	else
		hostname = gethostname()
	end
    jobname = "julia-$(getpid())"
    tdir = "$home/.julia-htc"
    run(`mkdir -p $tdir`)

	script_fname = "$tdir/$jobname.sh"
    scriptf = open(script_fname, "w")
    println(scriptf, "#!/bin/sh")
    println(scriptf, "$exehome/$exename --worker | /usr/bin/nc $hostname $portnum")
    close(scriptf)
	run(`chmod u+x $script_fname`)
    subf = open("$tdir/$jobname.sub", "w")
    println(subf, "executable = $tdir/$jobname.sh")
    println(subf, "universe = vanilla")
    println(subf, "should_transfer_files = yes")
	println(subf, "getenv = True")
    println(subf, "Notification = Error")
    println(subf, "priority = $priority")
    for i = 1:np
        println(subf, "output = $tdir/$jobname-$i.o")
        println(subf, "error= $tdir/$jobname-$i.e")
        println(subf, "queue")
    end
    close(subf)

    "$tdir/$jobname.sub"
end

function launch(manager::HTCManager, np::Integer, config::Dict, instances_arr::Array, c::Condition)
    try 
        portnum = rand(8000:9000)
        server = listen(portnum)

        script = condor_script(portnum, np, config) 
        out,proc = open(`condor_submit $script`)
        if !success(proc)
            println("batch queue not available (could not run condor_submit)")
            return
        end
        print(readline(out))
        print("Waiting for $np workers: ")

        io_objs = cell(np)
        configs = cell(np)
        for i=1:np
            conn = accept(server)
            io_objs[i] = conn
            configs[i] = merge(config, {:conn => conn, :server => server})
            print("$i ")
        end
        println(".")

        push!(instances_arr, collect(zip(io_objs, configs)))
        notify(c)
   catch e
        println("Error launching condor")
        println(e)
   end
end

function manage(manager::HTCManager, id::Integer, config::Dict, op::Symbol)
    if op == :finalize
        close(config[:conn])
#     elseif op == :interrupt
#         job = config[:job]
#         task = config[:task]
#         # this does not currently work
#         if !success(`qsig -s 2 -t $task $job`)
#             println("Error sending a Ctrl-C to julia worker $id (job: $job, task: $task)")
#         end
    end
end

addprocs_htc(np::Integer;kvs...) = addprocs(np, cman=HTCManager();kvs...)
