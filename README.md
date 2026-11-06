# hermes

Send a file or folder to another computer.

## Install

macOS / Linux:

    curl -fsSL https://raw.githubusercontent.com/ochaton/hermes/experiment/ux-ticket/install.sh | sh

Windows (PowerShell):

    irm https://raw.githubusercontent.com/ochaton/hermes/experiment/ux-ticket/install.ps1 | iex

## Use

    hermes send photo.jpg

Paste the printed line on the other computer.

Folders work the same way.

Agents: `hermes send --json <path>` and `hermes get --json <ticket>`.
Stdout events: ready, progress, done, error.
