$target_path = "D:\" # The volume with hard links to be repaired

$temp_location = "E:\" # Should be a different volume seperate from target

$api = "http://127.0.0.1:6666/" # Url and Port the message queue will run on 

$max_workers = 6 # Throttle jobs by this value

$sb_message_queue = {
    param ($api_)
    try {
        $pendingqueue = @()
        $listener = New-Object System.Net.HttpListener
        $listener.Prefixes.Add($api_) 
        $listener.Start()
        $running = $true
        while($running)
        {
            $message = $null
            $context = $listener.GetContext() 
            $request = $context.Request
            $endpoint = $request.Url
            $request
            if ($endpoint -match '/end') 
            { 
                $running = $false
                $message = "ok"
            }
            if ($endpoint -match '/set')
            {
                $sr = New-Object System.IO.StreamReader($request.InputStream, $request.ContentEncoding)
                $param = $sr.ReadToEnd()
                $pendingqueue += $param
                $pendingqueue
                $message = "ok"
            }
            if (($endpoint -match '/get') -and (!$pendingqueue))
            {
                $message = 'wait'
            }
            if (($endpoint -match '/get') -and ($pendingqueue))
            {
                $item = $pendingqueue[0]
                $pendingqueue = @($pendingqueue | ? { $_ –ne $item })
                $message = $item
            }
            if ($message)
            {
                $context.Response.StatusCode = 200
                $context.Response.ContentType = 'application/json'
                $responseBytes = [System.Text.Encoding]::UTF8.GetBytes($message)
                $context.Response.OutputStream.Write($responseBytes, 0, $responseBytes.Length)
                $context.Response.Close()
            }
        }
        $listener.Stop()
    } catch { Write-Host $_.ScriptStackTrace }
}

$sb_coordinator = {
    param ($api_, $directory)
    function crawl {
        param ($api__, $path_)
        try {
            Write-Host "Discovered $path_"
            $items = Get-ChildItem $path_ -Directory
            foreach ($item in $items) 
            {
                $folder = "$path_\$item"
                write-host $folder -foregroundcolor Red
                Write-Host "..."
                $endpoint = "/set"
                $request = $api__ + $endpoint
                $response = Invoke-RestMethod -Uri $request -Method Post -Body $folder
                crawl $api__ $folder  
            }

        } catch { Write-Host $_.ScriptStackTrace }
        return
    }
    crawl $api_ $directory
}

$sb_worker = {
    param ($api_, $temp_location_)
    $waiting = 0
    $running = $true
    while($running)
    {
        Write-Host "..."
        $endpoint = "/get"
        $request = $api_ + $endpoint
        $response = Invoke-RestMethod -Uri $request -Method Get
        if ($response -match 'wait')
        {
            if ($waiting -gt 3)
            {
                $running = $false
            }
            else
            {
                Start-Sleep -Seconds 10
                $waiting += 1
            }
        }
        else
        {
            $waiting = 0
            $path = $response
            Write-Host "Pricessing $path" -ForegroundColor Green
            try {
                $items = Get-ChildItem $path
                foreach ($item in $items) 
                {
                    $count = 0
                    $count = fsutil hardlink list "$($item.FullName)"
                    if ($count.count -gt 1)
                    {
                        $random = (-join ((65..90) + (97..122) | Get-Random -Count 16 | % {[char]$_}))
                        Write-Host "$path\$item" -foregroundcolor Yellow
                        Move-Item -Path "$path/$($item.Name)" -Destination "$temp_location_/$random"
                        Move-Item -Path "$temp_location_/$random" -Destination "$path/$($item.Name)"
                    }
                }
            } catch { Write-Host $_.ScriptStackTrace }
        }
    }
}

function main
{ 
    $jobs = @()
    Write-Host "Starting message queue ..."
    $msgq = Start-Job -ScriptBlock $sb_message_queue -ArgumentList $api
    Write-Host "Starting coordinator ..."
    $jobs += Start-Job -ScriptBlock $sb_coordinator -ArgumentList $api, $target_path
    $workers = 0
    while ($workers -lt $max_workers)
    {
        Write-Host "Starting worker ..."
        $workers += 1
        $jobs += Start-Job -ScriptBlock $sb_worker -ArgumentList $api, $temp_location
    }
    Wait-Job -Job $jobs > $null
    Write-Host "Finished"
    $ret = $jobs | Receive-Job
    $endpoint = "/end"
    $request = $api + $endpoint
    $response = Invoke-RestMethod -Uri $request -Method Get
    return
}

main
