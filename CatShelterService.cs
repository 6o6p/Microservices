using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microservices.Common.Exceptions;
using Microservices.ExternalServices.Authorization;
using Microservices.ExternalServices.Authorization.Types;
using Microservices.ExternalServices.Billing;
using Microservices.ExternalServices.Billing.Types;
using Microservices.ExternalServices.CatDb;
using Microservices.ExternalServices.CatDb.Types;
using Microservices.ExternalServices.CatExchange;
using Microservices.ExternalServices.CatExchange.Types;
using Microservices.ExternalServices.Database;
using Microservices.Types;

namespace Microservices
{
    public class CatShelterService : ICatShelterService
    {
        private readonly IDatabase _db;
        private readonly IAuthorizationService _authorizationService;
        private readonly IBillingService _billingService;
        private readonly ICatInfoService _catInfoService;
        private readonly ICatExchangeService _catExchangeService;

        public CatShelterService(
            IDatabase database,
            IAuthorizationService authorizationService,
            IBillingService billingService,
            ICatInfoService catInfoService,
            ICatExchangeService catExchangeService)
        {
            _db = database;
            _authorizationService = authorizationService;
            _billingService = billingService;
            _catInfoService = catInfoService;
            _catExchangeService = catExchangeService;
        }

        public async Task<List<Cat>> GetCatsAsync(string sessionId, int skip, int limit,
                                                  CancellationToken cancellationToken)
        {
            var user = await TryAuthorizeAsync(sessionId, cancellationToken);

            var billing = await TryConnect(2, () => _billingService.GetProductsAsync(skip, limit, cancellationToken));

            var catEntities = _db.GetCollection<CatEntity, Guid>("CatEntities"); //Можно коллекцию в поле прям в конструкторе сохранять

            return billing
                .Select(async product => await MakeCatAsync(catEntities, product.Id, cancellationToken))
                .Select(t => t.Result) //Вот так с асинхронным кодом точно делать не надо. Нет асинхронного LINQ - не надо использовать LINQ
                .ToList();
        }

        public async Task AddCatToFavouritesAsync(string sessionId, Guid catId, CancellationToken cancellationToken)
        {
            var user = await TryAuthorizeAsync(sessionId, cancellationToken);

            var userFavorites = await GetFavorites(user.UserId, cancellationToken) 
                                ?? new UserFavorites { Id = user.UserId, Favorites = new HashSet<Guid>() };
            
            userFavorites.Favorites.Add(catId);

            await WriteToCollectionAsync("Favorites", userFavorites, cancellationToken);
        }

        public async Task<List<Cat>> GetFavouriteCatsAsync(string sessionId, CancellationToken cancellationToken)
        {
            var user = await TryAuthorizeAsync(sessionId, cancellationToken);

            var catEntities = _db.GetCollection<CatEntity, Guid>("CatEntities");

            return (await GetFavorites(user.UserId, cancellationToken))?.Favorites?
                        .Where(id => GetProductAsync(id, cancellationToken).Result != null)
                        .Select(async id => await MakeCatAsync(catEntities, id, cancellationToken))
                        .Select(t => t.Result)
                        .ToList()
                   ?? new List<Cat>();
        }

        public async Task DeleteCatFromFavouritesAsync(string sessionId, Guid catId,
                                                       CancellationToken cancellationToken)
        {
            var user = await TryAuthorizeAsync(sessionId, cancellationToken);

            var userFavorites = await GetFavorites(user.UserId, cancellationToken);

            if (userFavorites?.Favorites is not null)
            {
                userFavorites.Favorites.Remove(catId);
                await WriteToCollectionAsync("Favorites", userFavorites, cancellationToken);
            }
        }

        public async Task<Bill> BuyCatAsync(string sessionId, Guid catId, CancellationToken cancellationToken)
        {
            var user = await TryAuthorizeAsync(sessionId, cancellationToken);

            var product = await GetProductAsync(catId, cancellationToken) ?? throw new InvalidRequestException();

            var catPrice = (await GetBreedPricesAsync(product.BreedId, cancellationToken))?.Last().Price ?? 1000;

            return await TryConnect(2, () => _billingService.SellProductAsync(catId, catPrice, cancellationToken));
        }

        public async Task<Guid> AddCatAsync(string sessionId, AddCatRequest request,
                                            CancellationToken cancellationToken)
        {
            var user = await TryAuthorizeAsync(sessionId, cancellationToken);

            var catInfo = await GetCatInfoAsync(request.Breed, cancellationToken);

            var catEntity = new CatEntity()
            {
                Id = Guid.NewGuid(),
                BreedId = catInfo.BreedId,
                AddedBy = user.UserId,
                Name = request.Name,
                CatPhoto = request.Photo
            };

            await TryConnect(2, () => _billingService.AddProductAsync(
                new Product { Id = catEntity.Id, BreedId = catEntity.BreedId },
                cancellationToken
            ));

            await WriteToCollectionAsync("CatEntities", catEntity, cancellationToken);

            return catEntity.Id;
        }

        private class CatEntity : IEntityWithId<Guid>
        {
            public Guid Id { get; set; }
            public Guid BreedId { get; set; }
            public Guid AddedBy { get; set; }
            public string Name { get; set; }
            public byte[] CatPhoto { get; set; }
        }

        private class UserFavorites : IEntityWithId<Guid>
        {
            public Guid Id { get; set; }
            public HashSet<Guid> Favorites { get; set; }
        }

        private async Task<T> TryConnect<T>(int tryCount, Func<Task<T>> func)
        {
            while (true) //Можно использовать цикл for
            {
                try
                {
                    return await func();
                }
                catch (ConnectionException)
                {
                    if (--tryCount == 0)
                        throw new InternalErrorException(); //Можно пойти чуть дальше и вынести обработку ошибок в общее место
                }
            }
        }

        private async Task TryConnect(int tryCount, Func<Task> func)
        {
            while (true)
            {
                try
                {
                    await func();
                    return;
                }
                catch (ConnectionException)
                {
                    if (--tryCount == 0)
                        throw new InternalErrorException();
                }
            }
        }

        private async Task<AuthorizationResult> TryAuthorizeAsync(string sessionId, CancellationToken cancellationToken)
        {
            var result = await TryConnect(2, () => _authorizationService.AuthorizeAsync(sessionId, cancellationToken));

            return result.IsSuccess
                ? result
                : throw new AuthorizationException();
        }

        private async Task<Product> GetProductAsync(Guid id, CancellationToken cancellationToken) =>
            await TryConnect(2, () => _billingService.GetProductAsync(id, cancellationToken));

        private async Task WriteToCollectionAsync<T>(string collection, T document, CancellationToken cancellationToken)
        where T : class, IEntityWithId<Guid> =>
            await TryConnect(2, () => _db.GetCollection<T, Guid>(collection).WriteAsync(document, cancellationToken));

        private async Task<UserFavorites> GetFavorites(Guid userId, CancellationToken cancellationToken) =>
            await TryConnect(2, () => _db.GetCollection<UserFavorites, Guid>("Favorites")
                                         .FindAsync(userId, cancellationToken));

        private async Task<CatInfo> GetCatInfoAsync(Guid breedId, CancellationToken cancellationToken) =>
            await TryConnect(2, () => _catInfoService.FindByBreedIdAsync(breedId, cancellationToken));

        private async Task<CatInfo> GetCatInfoAsync(string breedName, CancellationToken cancellationToken) =>
            await TryConnect(2, () => _catInfoService.FindByBreedNameAsync(breedName, cancellationToken));

        private async Task<List<(DateTime Date, decimal Price)>> GetBreedPricesAsync(Guid breedId,
                                                                        CancellationToken cancellationToken)
        {
            var history = await TryConnect(2, () => _catExchangeService.GetPriceInfoAsync(breedId, cancellationToken));

            return history.Prices.Count > 0
                ? history.Prices.Select(p => (p.Date, p.Price)).ToList()
                : null;
        }

        private async Task<Cat> MakeCatAsync(IDatabaseCollection<CatEntity, Guid> catEntities, 
                                             Guid catId, CancellationToken cancellationToken)
        {
            var catEntity = await TryConnect(2, () => catEntities.FindAsync(catId, cancellationToken)); 

            var catInfo = await GetCatInfoAsync(catEntity.BreedId, cancellationToken);

            var prices = await GetBreedPricesAsync(catInfo.BreedId, cancellationToken);

            return new Cat
            {
                Id = catEntity.Id,
                BreedId = catEntity.BreedId,
                AddedBy = catEntity.AddedBy,
                Breed = catInfo.BreedName,
                Name = catEntity.Name,
                CatPhoto = catEntity.CatPhoto,
                BreedPhoto = catInfo.Photo,
                Price = prices?.Last().Price ?? 1000,
                Prices = prices ?? new List<(DateTime Date, decimal Price)>()
            };
        }
    }
}
