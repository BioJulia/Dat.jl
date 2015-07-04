module Dat

# Dat is capable of storing tabular data, or Blob data:
# dat import, imports key/value data into dat.
#  dat import <filename> --dataset=<name>
# dat write, will write binary data into dat:
#  dat write <filename> --dataset=<dataset-name>

## Types for representing a Dat repositories and Dat datasets.

type DatRepository
    directory::String
    repositoryFile::String
    remote::String
    datCommand::Cmd

    function DatRepository(directory = mktempdir(), repo = ".dat", remote = "", command = `dat`)
        dir = normpath(expanduser(directory))
        if !isdir(dir)
            mkdir(dir)
        end
        repositoryFile = normpath("$dir/$repo")
        out = new(dir, repositoryFile, remote, command)
        if !isdir(out.repositoryFile)
            msg, err = datCommand(out, ` init`)
            info(err)
        end
        return out
    end
end


Base.cd(repo::DatRepository) = cd(repo.directory)


function datCommand(repo::DatRepository, arguments::Cmd)
    tempLog = tempname() * "_dat_messages.log"
    tempErr = tempname() * "_dat_errors.log"
    try
        cd(() -> run(pipe(`$(repo.datCommand) $arguments`, stdout = tempLog, stderr = tempErr)), repo.directory)
    catch exception
        stdout = readall(tempLog)
        stderr = readall(tempErr)
        if isa(exception, ErrorException)
            if stderr != ""
                info(stderr)
                error(stderr)
            end
        end
    end
    return (readall(tempLog), readall(tempErr))
end


function status(repo::DatRepository)
    out = ""
    msg, err = datCommand(repo, `status --json`)
    if msg == "{\"error\":true,\"message\":\"This dat is empty\"}\n"
        info("Dat repository is empty.")
    else
        out = msg
    end
    return out
end
















end
