# genpipe

Tiny data processing library for TypeScript based on composable async generators.

## Installation

### Copy the file

The library is just one file - [genpipe.ts](genpipe.ts). Copy it to your project and use it.

### Install with npm

```sh
npm install genpipe
```

## Usage

### Basic example

```typescript
import { Pipeline } from './genpipe'

const x = new Pipeline(toGen([20, 5, 2, 1]))
    .tf(F.map((i) => i * 2))
    .toAsync()
    .tf(F.mapConcurrent(10, async (i) => {
        const timeout = 1000 * Math.max(0.5, Math.random())
        console.log(`starting timeout async function ${i}`)
        await delay(timeout)
        console.log(`finished timeout async function ${i}`)
        return i
    }))
    .eval()

console.log(await x)
```
